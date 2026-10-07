package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.RunControlTracker.Decision;
import com.kmwllc.lucille.message.CrawlerMessenger;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import sun.misc.Signal;

/**
 * Executes the work units of distributed crawls. A Crawler stays alive indefinitely, polling for units from any run.
 * For each unit it instantiates the connector the unit names from its own config, has that connector publish the
 * unit's Documents, and reports to the run's Coordinator whether the unit succeeded.
 */
class Crawler implements Runnable {

  public static final String UNIT_ID_FIELD = "unit_id";
  public static final String METRICS_PREFIX = "crawler";

  private static final Logger log = LoggerFactory.getLogger(Crawler.class);
  private static final long ERROR_PAUSE_MILLIS = 1000;
  private static final int MAX_ERROR_LENGTH = 500;
  private static final int HANDED_BACK_PER_EVENT = 200;
  private static final int HANDED_BACK_CHARS_PER_EVENT = 100_000;

  private final Config config;
  private final CrawlerMessenger messenger;
  private final String crawlerName;
  private final long heartbeatWaitMillis;
  private final boolean dispatchHandBacksEarly;
  private final Set<String> pipelineNames;

  private final Timer unitTimer;
  private final com.codahale.metrics.Meter docMeter;
  private final com.codahale.metrics.Counter abandonedUnits;

  private volatile boolean running = true;

  // The unit being executed, if any, and when its execution began. Read by the pool's watchdog thread.
  private volatile WorkUnit executingUnit;
  private volatile long executingSinceMillis;
  private volatile Execution currentExecution;
  // Guards the decision to report the executing unit: its own thread and the watchdog must not both do so.
  private final Object reportLock = new Object();
  private boolean executingUnitReported;

  // The connector for the most recent unit, kept so that consecutive units of the same connector and run share its
  // connections. Closed when a unit for another connector or run arrives.
  private PartitionableConnector cachedConnector;
  private String cachedConnectorKey;
  private String cachedConfigHash;

  Crawler(Config config, CrawlerMessenger messenger, String crawlerName) {
    this.config = config;
    this.messenger = messenger;
    this.crawlerName = crawlerName;
    // A unit can arrive just ahead of the first heartbeat of its run or epoch, so the heartbeat is given a few periods
    // to turn up. No longer: a unit for a run that does not exist would hold up the units behind it for that time.
    CrawlConfig crawlConfig = new CrawlConfig(config);
    this.heartbeatWaitMillis =
        TimeUnit.SECONDS.toMillis(Math.min(crawlConfig.orphanTimeoutSecs, 3L * crawlConfig.heartbeatSecs));
    this.dispatchHandBacksEarly = crawlConfig.dispatchHandBacksEarly;
    this.pipelineNames = config.getConfigList("pipelines").stream()
        .map(pipeline -> pipeline.getString("name")).collect(Collectors.toSet());

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.unitTimer = metrics.timer(METRICS_PREFIX + ".unit.duration");
    this.docMeter = metrics.meter(METRICS_PREFIX + ".docs.published");
    this.abandonedUnits = metrics.counter(METRICS_PREFIX + ".units.abandoned");
  }

  public void terminate() {
    log.debug("terminate called");
    running = false;
  }

  @Override
  public void run() {
    try {
      pollForUnits();
    } finally {
      // Closing the messenger gives up any unit still held. This must happen however the thread ends: a unit held by
      // a thread that has died would otherwise never be delivered to another Crawler.
      closeCachedConnector();
      messenger.close();
      log.debug("Exiting");
    }
  }

  private void pollForUnits() {
    while (running) {
      WorkUnit unit;
      try {
        unit = messenger.pollWorkUnit();
      } catch (Exception e) {
        log.error("Crawler could not poll for a work unit; will retry.", e);
        pauseAfterError();
        continue;
      }

      if (unit == null) {
        continue;
      }

      MDC.put(RUNID_FIELD, unit.runId());
      MDC.put(UNIT_ID_FIELD, unit.unitId());
      try {
        handleUnit(unit);
      } catch (Exception e) {
        log.error("Crawler could not report on unit {}; it will be delivered again.", unit.unitId(), e);
        messenger.releaseWorkUnit();
        pauseAfterError();
      } finally {
        MDC.remove(RUNID_FIELD);
        MDC.remove(UNIT_ID_FIELD);
      }
    }
  }

  private void pauseAfterError() {
    try {
      Thread.sleep(ERROR_PAUSE_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running = false;
    }
  }

  /**
   * Executes the unit if its run is alive, tells the Coordinator how it went, and acknowledges it.
   */
  private void handleUnit(WorkUnit unit) throws Exception {
    // The unit's run ID and pipeline name decide which topic this Crawler reports to. Units arrive over the network,
    // so both are checked before anything is sent anywhere on the unit's behalf.
    if (!CrawlConfig.isValidRunId(unit.runId()) || !pipelineNames.contains(unit.pipelineName())) {
      log.error("Discarding unit {}: its run ID is malformed or its pipeline {} is not in this Crawler's config.",
          unit.unitId(), unit.pipelineName());
      abandonedUnits.inc();
      messenger.ackWorkUnit();
      return;
    }

    Decision decision = messenger.getRunControlTracker().awaitDecision(unit.runId(), unit.epoch(), heartbeatWaitMillis);

    if (decision != Decision.RUN) {
      abandon(unit, decision);
      return;
    }

    log.info("Executing unit {} (attempt {}).", unit.unitId(), unit.attempt());
    Timer.Context timing = unitTimer.time();
    CrawlerPublisher publisher = null;
    Execution execution = new Execution(unit.runId());
    String error = null;
    Throwable failure = null;
    VirtualMachineError fatal = null;

    synchronized (reportLock) {
      executingUnitReported = false;
    }
    executingSinceMillis = System.currentTimeMillis();
    currentExecution = execution;
    executingUnit = unit;

    try {
      PartitionableConnector connector = connectorFor(unit);
      publisher = new CrawlerPublisher(messenger, unit, connector.getPipelineName(), connector.requiresCollapsingPublisher());
      execution.publisher = publisher;
      connector.executeUnit(unit, publisher, execution);
      publisher.flush();
    } catch (Throwable t) {
      // Errors are caught as well as Exceptions so that the failure is reported and counted against the unit's
      // attempts. A unit that killed its Crawler without a report would be delivered to the next one, and the next.
      log.error("Unit {} failed.", unit.unitId(), t);
      error = describe(t);
      failure = t;
      fatal = t instanceof VirtualMachineError ? (VirtualMachineError) t : null;
      // a connector that failed part way through may be in no state to execute another unit
      closeCachedConnector();
    }

    long durationMillis = TimeUnit.NANOSECONDS.toMillis(timing.stop());
    long numPublished = publisher == null ? 0 : publisher.numPublished();
    docMeter.mark(numPublished);

    // from here on the watchdog leaves the unit alone; if it has already reported it, there is nothing more to say
    boolean reportedByWatchdog;
    synchronized (reportLock) {
      executingUnit = null;
      reportedByWatchdog = executingUnitReported;
      executingUnitReported = true;
    }

    if (reportedByWatchdog) {
      // the watchdog acknowledged the unit and closed the messenger, and this Crawler has been replaced
      log.warn("Unit {} finished after it was reported as timed out; its result is not reported.", unit.unitId());
      return;
    } else if (publisher != null && publisher.isUnitLost()) {
      log.warn("Unit {} was reassigned while executing; another Crawler will execute it.", unit.unitId());
      abandonedUnits.inc();
    } else if (publisher != null && publisher.getAbortReason() != null) {
      abandon(unit, publisher.getAbortReason());
      return;
    } else {
      ObjectNode message = unitMessage(unit)
          .put(CoordinatorPublisher.EXECUTION, execution.id)
          .put(CoordinatorPublisher.NUM_PUBLISHED, numPublished)
          .put(CoordinatorPublisher.SOURCE_CALLS, execution.sourceCalls.get())
          .put(CoordinatorPublisher.REQUESTS, execution.requests.get())
          .put(CoordinatorPublisher.THROTTLED_CALLS, execution.throttledCalls.get())
          .put(CoordinatorPublisher.REFUSED_CALLS, execution.refusedCalls.get())
          .put(CoordinatorPublisher.DURATION_MS, durationMillis);
      if (error == null) {
        if (execution.errorClass != null) {
          // the unit completed by handing back what its source would not let it do
          message.put(CoordinatorPublisher.ERROR_CLASS, execution.errorClass.name())
              .put(CoordinatorPublisher.ERROR_CAUSE, truncate(execution.errorCause));
        }
        // under the execution's lock, so that a progress report sending parts early cannot send them after this
        synchronized (execution) {
          execution.completing = true;
          sendHandedBack(unit, execution,
              List.copyOf(execution.handedBack.subList(execution.sentHandedBack, execution.handedBack.size())), false);
          sendUnitEvent(unit, message.put(CoordinatorPublisher.NUM_CHILDREN, execution.handedBack.size()), Event.Type.UNIT_DONE);
        }
        log.info("Unit {} done: {} docs in {} ms, {} parts handed back, {} calls refused{}.", unit.unitId(), numPublished,
            durationMillis, execution.handedBack.size(), execution.refusedCalls.get(),
            execution.errorClass == null ? "" : ", ended by " + execution.errorClass);
      } else {
        message.put(CoordinatorPublisher.ERROR, error)
            .put(CoordinatorPublisher.ERROR_CLASS, FailureClass.of(failure).name())
            .put(CoordinatorPublisher.ERROR_CAUSE, describe(FailureClass.rootCause(failure)));
        execution.stopSendingEarly();
        sendUnitEvent(unit, message, Event.Type.UNIT_FAILED);
      }
    }

    messenger.ackWorkUnit();

    // the JVM is out of memory or otherwise broken; this thread should not take another unit
    if (fatal != null) {
      throw fatal;
    }
  }

  /**
   * What one execution of a unit handed back and how many calls it made to its source. Each execution has an ID of
   * its own, because a unit can be executed more than once and the Coordinator must not mix their reports.
   */
  private class Execution implements UnitContext {

    final String id = UUID.randomUUID().toString();
    final String runId;
    // what the last UNIT_PROGRESS said, so that one is sent only when there is something new to say
    long reportedSourceCalls = 0;
    long reportedRefusedCalls = 0;
    long reportedThrottledCalls = 0;
    long reportedRequests = 0;
    // How many of the handed-back parts have been sent early, and whether the completion has taken over sending the
    // rest. Guarded by the Execution's own lock.
    int sentHandedBack = 0;
    boolean completing = false;

    Execution(String runId) {
      this.runId = runId;
    }
    final List<ObjectNode> handedBack = new CopyOnWriteArrayList<>();
    // a part handed back twice is one part; counting it twice would make the Coordinator expect one it never gets
    private final Set<String> handedBackKeys = ConcurrentHashMap.newKeySet();
    final AtomicLong sourceCalls = new AtomicLong();
    final AtomicLong refusedCalls = new AtomicLong();
    final AtomicLong throttledCalls = new AtomicLong();
    final AtomicLong requests = new AtomicLong();
    volatile FailureClass errorClass;
    volatile String errorCause;
    // set once the unit has a publisher, which is what learns that the unit was lost or its run stopped
    volatile CrawlerPublisher publisher;
    volatile boolean timedOut = false;

    @Override
    public void handBack(String unitKey, ObjectNode payload) {
      if (!handedBackKeys.add(unitKey)) {
        return;
      }
      ObjectNode child = CrawlConfig.newMessage().put(CoordinatorPublisher.CHILD_KEY, unitKey);
      child.set(CoordinatorPublisher.CHILD_PAYLOAD, payload);
      handedBack.add(child);
    }

    /**
     * Ends early sending before the unit is reported as failed, so that no early part follows the failure: the
     * Coordinator would take it as handed back by the next attempt. Waits for an early send under way.
     */
    void stopSendingEarly() {
      synchronized (this) {
        completing = true;
      }
    }

    @Override
    public void addSourceCalls(long calls) {
      sourceCalls.addAndGet(calls);
    }

    @Override
    public void addRefusedCalls(long calls) {
      addRefusedCalls(calls, FailureClass.THROTTLED);
    }

    @Override
    public void addRequests(long count) {
      requests.addAndGet(count);
    }

    @Override
    public void addRefusedCalls(long calls, FailureClass failureClass) {
      // refusals before throttles, and reports read them the other way round, so a report never has more throttles
      // than refusals
      refusedCalls.addAndGet(calls);
      if (failureClass == FailureClass.THROTTLED) {
        throttledCalls.addAndGet(calls);
      }
    }

    @Override
    public Integer maxSourceConcurrency() {
      return messenger.getRunControlTracker().unitConcurrency(runId);
    }

    @Override
    public void recordSourceError(FailureClass failureClass, String cause) {
      errorClass = failureClass;
      errorCause = cause;
    }

    @Override
    public boolean isCancelled() {
      CrawlerPublisher current = publisher;
      return timedOut || (current != null && (current.isUnitLost() || current.getAbortReason() != null));
    }
  }

  // Sent ahead of the unit's completion, a batch at a time so that no Event grows with the size of a directory.
  // Each batch sent is counted in the execution, so that if a send fails, what follows does not send it again.
  private void sendHandedBack(WorkUnit unit, Execution execution, List<ObjectNode> parts, boolean early) throws Exception {
    List<ObjectNode> batch = new ArrayList<>();
    int batchChars = 0;

    for (ObjectNode part : parts) {
      batch.add(part);
      batchChars += part.toString().length();
      if (batch.size() >= HANDED_BACK_PER_EVENT || batchChars >= HANDED_BACK_CHARS_PER_EVENT) {
        sendHandedBackBatch(unit, execution, batch, early);
        execution.sentHandedBack += batch.size();
        batch = new ArrayList<>();
        batchChars = 0;
      }
    }

    if (!batch.isEmpty()) {
      sendHandedBackBatch(unit, execution, batch, early);
      execution.sentHandedBack += batch.size();
    }
  }

  private void sendHandedBackBatch(WorkUnit unit, Execution execution, List<ObjectNode> parts, boolean early) throws Exception {
    ObjectNode message = unitMessage(unit).put(CoordinatorPublisher.EXECUTION, execution.id);
    if (early) {
      message.put(CoordinatorPublisher.EARLY, true);
    }
    message.putArray(CoordinatorPublisher.CHILDREN).addAll(parts);
    sendUnitEvent(unit, message, Event.Type.UNIT_CHILDREN);
  }

  /**
   * Sends what the unit has handed back since the last time, marked to be dispatched at once. Nothing is sent once
   * the source has failed the unit: what it hands back from then on is what the source refused, which waits for the
   * unit to complete and is then delayed. The parts are counted before the failure is looked for, so that a part
   * handed back after the failure was recorded is never among them.
   */
  private void sendHandedBackEarly(WorkUnit unit, Execution execution) throws Exception {
    synchronized (execution) {
      int available = execution.handedBack.size();
      if (execution.completing || execution.errorClass != null || execution.isCancelled()
          || available <= execution.sentHandedBack) {
        return;
      }
      // a copy: sending advances sentHandedBack
      List<ObjectNode> parts = List.copyOf(execution.handedBack.subList(execution.sentHandedBack, available));
      sendHandedBack(unit, execution, parts, true);
      log.info("Unit {} handed back {} parts while executing.", unit.unitId(), parts.size());
    }
  }

  /**
   * Sends a UNIT_PROGRESS report for the unit being executed, if there is one and it has done anything since the
   * last report: the calls it has made to its source and how many were refused. Called from the pool's timer every
   * heartbeat, so that the Coordinator learns of refusals while a long unit is still running, not only when it ends.
   * With crawl.dispatchHandBacksEarly, also sends the parts the unit has handed back since, to be dispatched at once.
   */
  void reportProgress() {
    WorkUnit unit = executingUnit;
    Execution execution = currentExecution;
    if (unit == null || execution == null) {
      return;
    }

    long sourceCalls = execution.sourceCalls.get();
    long requests = execution.requests.get();
    long throttledCalls = execution.throttledCalls.get();
    long refusedCalls = execution.refusedCalls.get();
    boolean callsChanged;
    synchronized (reportLock) {
      if (executingUnit != unit || executingUnitReported) {
        return;
      }
      // the throttles too: a report can catch a refusal before its throttle, and the throttle must still go out
      callsChanged = sourceCalls != execution.reportedSourceCalls || refusedCalls != execution.reportedRefusedCalls
          || throttledCalls != execution.reportedThrottledCalls || requests != execution.reportedRequests;
      execution.reportedRequests = requests;
      execution.reportedSourceCalls = sourceCalls;
      execution.reportedRefusedCalls = refusedCalls;
      execution.reportedThrottledCalls = throttledCalls;
    }

    try {
      if (callsChanged) {
        ObjectNode message = unitMessage(unit)
            .put(CoordinatorPublisher.EXECUTION, execution.id)
            .put(CoordinatorPublisher.SOURCE_CALLS, sourceCalls)
            .put(CoordinatorPublisher.REQUESTS, requests)
            .put(CoordinatorPublisher.REFUSED_CALLS, refusedCalls)
            .put(CoordinatorPublisher.THROTTLED_CALLS, throttledCalls)
            .put(CoordinatorPublisher.NUM_PUBLISHED, execution.publisher == null ? 0 : execution.publisher.numPublished());
        sendUnitEvent(unit, message, Event.Type.UNIT_PROGRESS);
      }
      if (dispatchHandBacksEarly) {
        sendHandedBackEarly(unit, execution);
      }
    } catch (Exception e) {
      log.warn("Could not report progress on unit {}.", unit.unitId(), e);
    }
  }

  /**
   * Reports the unit being executed as failed if it has been executing for longer than the given time, and retires
   * this Crawler. Called from the pool's watchdog thread.
   *
   * A connector that is blocked cannot be made to stop, so the unit's thread is left as it is. What can be done is
   * done around it: the Coordinator is told, so that it dispatches the unit again and counts the hang against its
   * attempts; the unit is acknowledged, so that this attempt is not delivered to another Crawler to hang that one
   * too; and the messenger is closed, which gives up this Crawler's share of the work topic, where units, possibly
   * including the one just reported, would otherwise wait behind a thread that is never coming back.
   *
   * @return whether a unit was reported. If so this Crawler takes no more units and the pool should replace it.
   */
  boolean reportIfTimedOut(long maxUnitMillis) {
    WorkUnit unit = executingUnit;
    if (unit == null || System.currentTimeMillis() - executingSinceMillis <= maxUnitMillis) {
      return false;
    }

    Execution execution;
    synchronized (reportLock) {
      if (executingUnit != unit || executingUnitReported) {
        return false;
      }
      executingUnitReported = true;
      execution = currentExecution;
      execution.timedOut = true;
    }

    log.error("Unit {} has been executing for more than {} ms; reporting it as failed and retiring its Crawler.",
        unit.unitId(), maxUnitMillis);
    abandonedUnits.inc();
    running = false;
    execution.stopSendingEarly();
    try {
      ObjectNode message = unitMessage(unit)
          .put(CoordinatorPublisher.ERROR, "Timed out: still executing after " + maxUnitMillis + " ms.")
          .put(CoordinatorPublisher.ERROR_CLASS, FailureClass.TIMEOUT.name());
      sendUnitEvent(unit, message, Event.Type.UNIT_FAILED);
      messenger.ackWorkUnit();
    } catch (Exception e) {
      // unacknowledged, the unit is delivered to another Crawler, which reports on it in its turn
      log.error("Could not report that unit {} timed out.", unit.unitId(), e);
    }
    messenger.close();
    return true;
  }

  /**
   * Describes a failure for the Coordinator. The text is written to Kafka and to the Coordinator's log, so it is
   * kept short and on one line; the full exception is in this Crawler's own log.
   */
  private static String describe(Throwable t) {
    return truncate(t.getClass().getSimpleName() + ": " + t.getMessage());
  }

  private static String truncate(String text) {
    if (text == null) {
      return null;
    }
    text = text.replaceAll("\\p{Cntrl}", " ");
    return text.length() > MAX_ERROR_LENGTH ? text.substring(0, MAX_ERROR_LENGTH) + "..." : text;
  }

  /**
   * Gives up on a unit that should not be, or should no longer be, executed. The Coordinator of an orphaned run may
   * in fact be alive and merely slow to send heartbeats, so it is told that the unit was not completed; a run that
   * was cancelled or taken over by a newer epoch is not waiting to hear about the unit.
   */
  private void abandon(WorkUnit unit, Decision reason) throws Exception {
    log.info("Abandoning unit {}: run is {}.", unit.unitId(), reason);
    abandonedUnits.inc();

    // Nothing is reported for a run this Crawler has never heard from: there may be no such run, and no topic to
    // report to. And if the report cannot be sent, the unit is given up all the same. Holding on to it would block
    // every unit behind it, and a Coordinator that resumes the run dispatches its outstanding units again anyway.
    if (reason == Decision.ORPHANED) {
      try {
        ObjectNode message = unitMessage(unit)
            .put(CoordinatorPublisher.ERROR, "Abandoned: no heartbeat from the run's Coordinator.");
        sendUnitEvent(unit, message, Event.Type.UNIT_FAILED);
      } catch (Exception e) {
        log.warn("Could not report that unit {} was abandoned.", unit.unitId(), e);
      }
    }

    messenger.ackWorkUnit();
  }

  private ObjectNode unitMessage(WorkUnit unit) {
    return CrawlConfig.newMessage().put(CoordinatorPublisher.ATTEMPT, unit.attempt())
        .put(CoordinatorPublisher.EPOCH, unit.epoch()).put(CoordinatorPublisher.CRAWLER, crawlerName);
  }

  private void sendUnitEvent(WorkUnit unit, ObjectNode message, Event.Type type) throws Exception {
    messenger.sendEvent(new Event(unit.unitId(), unit.runId(), message.toString(), type), unit.pipelineName());
  }

  /**
   * Returns a connector for the unit, built from this Crawler's own config. Only the connector's name is taken from
   * the unit; the class to instantiate and its settings are not.
   */
  private PartitionableConnector connectorFor(WorkUnit unit) throws Exception {
    String key = unit.runId() + "/" + unit.connectorName();

    if (!key.equals(cachedConnectorKey)) {
      closeCachedConnector();

      Config connectorConfig = CrawlConfig.connectorConfig(config, unit.connectorName());
      if (connectorConfig == null) {
        throw new ConnectorException("No connector named " + unit.connectorName() + " in this Crawler's config.");
      }

      Class<?> clazz = Class.forName(connectorConfig.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      cachedConnector = SingleUnitAdapter.wrap((Connector) constructor.newInstance(connectorConfig));
      cachedConnectorKey = key;
      cachedConfigHash = CrawlConfig.connectorConfigHash(connectorConfig);
    }

    if (!cachedConfigHash.equals(unit.configHash())) {
      throw new ConnectorException("Config for connector " + unit.connectorName()
          + " differs between this Crawler and the run's Coordinator.");
    }

    return cachedConnector;
  }

  private void closeCachedConnector() {
    if (cachedConnector != null) {
      try {
        cachedConnector.close();
      } catch (Exception e) {
        log.error("Error closing connector", e);
      }
    }
    cachedConnector = null;
    cachedConnectorKey = null;
    cachedConfigHash = null;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    CrawlerPool crawlerPool = new CrawlerPool(config, CrawlerMessengerFactory.getKafkaFactory(config));
    crawlerPool.start();

    // TERM is handled as well as INT because it is what a container runtime sends. A Crawler that exits without
    // leaving its consumer group keeps its unit from other Crawlers until its session times out.
    for (String signalName : new String[] {"INT", "TERM"}) {
      Signal.handle(new Signal(signalName), signal -> {
        crawlerPool.stop();
        log.info("Crawlers shutting down");
        try {
          crawlerPool.join();
        } catch (InterruptedException e) {
          log.error("Interrupted", e);
        }
        System.exit(0);
      });
    }
  }
}
