package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.CoordinatorMessenger;
import com.kmwllc.lucille.message.KafkaCoordinatorMessenger;
import com.kmwllc.lucille.message.KafkaRunControl;
import com.kmwllc.lucille.message.RunControl;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Coordinates a distributed crawl: a run in which the connectors' work is executed by separate Crawler processes
 * rather than by the Runner. As in any run, connectors are handled one at a time and the run stops if one fails.
 *
 * For each connector, the Coordinator calls the connector's lifecycle methods, has the connector plan its work units,
 * dispatches the units to Crawlers, and waits until every unit is done and every Document the Crawlers published has
 * been indexed or has failed. It does not publish the connector's Documents itself.
 *
 * Throughout the run the Coordinator sends a heartbeat. Crawlers stop working for a run whose heartbeat stops. Such a
 * run can be resumed by a new Coordinator given the same run ID, which picks up where the first one stopped.
 */
public class CrawlCoordinator {

  private static final Logger log = LoggerFactory.getLogger(CrawlCoordinator.class);

  private final Config config;
  private final String runId;
  private final RunControl runControl;
  private final Supplier<CoordinatorMessenger> messengerFactory;
  private final CrawlConfig crawlConfig;
  private final String configHash;

  private int epoch;
  // true from when this Coordinator takes charge of the run until it announces the end of the run
  private volatile boolean runActive = false;
  private volatile CoordinatorPublisher currentPublisher;

  // the first connector that has a pipeline, and the Publisher that was set up for it before the run was announced
  private Connector preparedConnector;
  private CoordinatorPublisher preparedPublisher;

  /**
   * Creates a Coordinator that communicates through Kafka.
   *
   * @param resume whether the run was started by an earlier Coordinator and should be continued.
   */
  public static CrawlCoordinator forKafka(Config config, String runId, boolean resume) throws Exception {
    return new CrawlCoordinator(config, runId, new KafkaRunControl(config),
        () -> new KafkaCoordinatorMessenger(config, resume));
  }

  /**
   * @param messengerFactory creates the messenger for each connector. When resuming, the messengers must replay the
   *                         run's existing Events.
   */
  public CrawlCoordinator(Config config, String runId, RunControl runControl,
      Supplier<CoordinatorMessenger> messengerFactory) {
    this.config = config;
    this.runId = runId;
    this.runControl = runControl;
    this.messengerFactory = messengerFactory;
    this.crawlConfig = new CrawlConfig(config);
    this.configHash = CrawlConfig.runConfigHash(config);
  }

  /**
   * Executes the run.
   *
   * @param resume whether to continue a run that an earlier Coordinator started, instead of starting a new one.
   * @param force when resuming, whether to proceed even though the earlier Coordinator may still be running.
   */
  public RunResult run(boolean resume, boolean force) throws Exception {
    MDC.put(Document.RUNID_FIELD, runId);
    List<Connector> connectors = Connector.fromConfig(config);
    List<ConnectorResult> connectorResults = new ArrayList<>();
    ScheduledExecutorService heartbeat = null;
    boolean status = false;

    try {
      String refusal = claimRun(resume, force);
      if (refusal != null) {
        log.error(refusal);
        return new RunResult(false, connectors, connectorResults, runId);
      }

      // The first connector's Publisher is set up before the run is announced, because setting it up is what can fail
      // for reasons outside the run: Kafka unreachable, or a topic that cannot be read. Failing here leaves no record
      // of the run, so it can be started again under the same ID.
      Connector first = connectors.stream().filter(c -> c.getPipelineName() != null).findFirst().orElse(null);
      if (first != null) {
        try {
          preparedPublisher = newPublisher(first);
          preparedConnector = first;
        } catch (Exception e) {
          log.error("Could not start run " + runId + ".", e);
          connectorResults.add(new ConnectorResult(first, null, false, String.valueOf(e.getMessage())));
          return new RunResult(false, connectors, connectorResults, runId);
        }
      }

      // Crawlers ignore units until they have seen a heartbeat for the unit's epoch, so send one before dispatching any.
      runControl.heartbeat(runId, epoch, configHash);
      runActive = true;
      heartbeat = startHeartbeat();
      Runner.onInterrupt(() -> endRun("interrupted"));

      status = runConnectors(connectors, connectorResults, resume);
      return new RunResult(status, connectors, connectorResults, runId);
    } finally {
      if (heartbeat != null) {
        heartbeat.shutdownNow();
      }
      endRun(status ? "complete" : "failed");
      // set up but never handed to its connector, if the run could not be announced
      close(preparedPublisher, "publisher");
      runControl.close();
    }
  }

  /**
   * Decides the epoch for this Coordinator, or returns a message explaining why it must not handle the run.
   */
  private String claimRun(boolean resume, boolean force) throws Exception {
    if (!CrawlConfig.isValidRunId(runId)) {
      return "Run ID " + runId + " cannot be used: it may contain only letters, digits, '.', '_' and '-', up to 128 of them.";
    }

    RunControl.Status previous = runControl.latest(runId);

    if (!resume) {
      epoch = 1;
      return previous == null ? null
          : "Run ID " + runId + " has been used before. Use -resume to continue that run, or a different run ID.";
    }

    if (previous == null) {
      return "Cannot resume run " + runId + ": no record of it was found.";
    }
    if (!configHash.equals(previous.configHash())) {
      return "Cannot resume run " + runId + ": the connectors in this config differ from those it was started with.";
    }
    if (!force && !previous.cancelled() && previous.ageMillis() < TimeUnit.SECONDS.toMillis(crawlConfig.orphanTimeoutSecs)) {
      return "Cannot resume run " + runId + ": its Coordinator sent a heartbeat " + previous.ageMillis()
          + " ms ago and may still be running. Use -force to resume regardless.";
    }

    if (previous.epoch() < 1 || previous.epoch() == Integer.MAX_VALUE) {
      return "Cannot resume run " + runId + ": its last control record has the impossible epoch " + previous.epoch() + ".";
    }

    epoch = previous.epoch() + 1;
    log.info("Resuming run {} as epoch {}.", runId, epoch);
    return null;
  }

  private ScheduledExecutorService startHeartbeat() {
    BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("CoordinatorHeartbeat", runId)).daemon(true).build();
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);

    executor.scheduleAtFixedRate(() -> {
      try {
        if (runActive && !supersededOrCancelled()) {
          runControl.heartbeat(runId, epoch, configHash);
        }
      } catch (Exception e) {
        log.error("Could not send heartbeat for run {}.", runId, e);
      }
    }, crawlConfig.heartbeatSecs, crawlConfig.heartbeatSecs, TimeUnit.SECONDS);

    return executor;
  }

  /**
   * Checks whether the run has been cancelled, or resumed by another Coordinator, since this one took charge of it.
   * If so, Crawlers are already discarding this Coordinator's units, so it stops instead of waiting for them.
   */
  private boolean supersededOrCancelled() throws Exception {
    RunControl.Status latest = runControl.latest(runId);
    if (latest == null || latest.epoch() < epoch || (latest.epoch() == epoch && !latest.cancelled())) {
      return false;
    }

    String reason = latest.epoch() > epoch
        ? "Run was taken over by another Coordinator (epoch " + latest.epoch() + ")."
        : "Run was cancelled.";
    log.error(reason);
    // nothing more is announced for the run: it now belongs to whoever cancelled it or took it over
    runActive = false;

    CoordinatorPublisher publisher = currentPublisher;
    if (publisher != null) {
      publisher.fail(reason);
    }
    return true;
  }

  // Tells Crawlers to discard whatever units of this run they still hold or receive. A later resume starts a new epoch.
  private synchronized void endRun(String reason) {
    if (!runActive) {
      return;
    }
    runActive = false;

    try {
      runControl.cancel(runId, epoch, configHash, reason);
    } catch (Exception e) {
      log.error("Could not announce the end of run {}.", runId, e);
    }
  }

  private boolean runConnectors(List<Connector> connectors, List<ConnectorResult> connectorResults, boolean resume) {
    for (Connector connector : connectors) {
      ConnectorResult result = connector.getPipelineName() == null
          ? Runner.runConnector(config, runId, connector, null)
          : runConnector(connector, resume);
      connectorResults.add(result);

      if (!result.getStatus()) {
        log.error("Aborting run because " + connector.getName() + " failed.");
        return false;
      }
    }

    return true;
  }

  private ConnectorResult runConnector(Connector connector, boolean resume) {
    CoordinatorPublisher publisher = null;

    try {
      if (connector == preparedConnector) {
        publisher = preparedPublisher;
        preparedPublisher = null;
      } else {
        publisher = newPublisher(connector);
      }
      currentPublisher = publisher;
      if (!runActive) {
        return new ConnectorResult(connector, publisher, false, "Run was cancelled or taken over by another Coordinator.");
      }
      return runConnector(SingleUnitAdapter.wrap(connector), publisher, resume);
    } catch (Exception e) {
      log.error("Connector " + connector.getName() + " failed.", e);
      return new ConnectorResult(connector, publisher, false, String.valueOf(e.getMessage()));
    } finally {
      currentPublisher = null;
      close(connector, "connector");
      close(publisher, "publisher");
    }
  }

  private CoordinatorPublisher newPublisher(Connector connector) throws Exception {
    String metricsPrefix = runId + "." + connector.getName() + "." + connector.getPipelineName();
    return new CoordinatorPublisher(config, messengerFactory.get(), runId, connector, metricsPrefix, epoch);
  }

  private static void close(Object closeable, String description) {
    try {
      if (closeable instanceof Connector connector) {
        connector.close();
      } else if (closeable instanceof Publisher publisher) {
        publisher.close();
      }
    } catch (Exception e) {
      log.error("Error closing " + description, e);
    }
  }

  /**
   * Takes one connector through its lifecycle. When resuming, each step that the run's log shows as already done
   * is skipped.
   */
  private ConnectorResult runConnector(PartitionableConnector connector, CoordinatorPublisher publisher, boolean resume)
      throws Exception {
    StopWatch stopWatch = StopWatch.createStarted();

    if (resume) {
      publisher.recover();
    }

    if (publisher.isHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE)) {
      log.info("Connector {} was completed earlier in this run.", connector.getName());
      return new ConnectorResult(connector, publisher, true, null);
    }

    log.info("Coordinating connector {} feeding to pipeline {}", connector.getName(), connector.getPipelineName());

    if (!publisher.isHookDone(CoordinatorPublisher.HOOK_PRE_EXECUTE)) {
      connector.preExecute(runId);
      publisher.logHookDone(CoordinatorPublisher.HOOK_PRE_EXECUTE);
    }

    if (!publisher.isHookDone(CoordinatorPublisher.HOOK_PREPARE_RUN)) {
      connector.prepareRun(runId);
      publisher.logHookDone(CoordinatorPublisher.HOOK_PREPARE_RUN);
    }

    publisher.redispatchOutstandingUnits();

    int timeout = ConfigUtils.getOrDefault(config, "runner.connectorTimeout", Runner.DEFAULT_CONNECTOR_TIMEOUT);
    CoordinatorThread planner = new CoordinatorThread("Planner", () -> {
      if (!publisher.isPlanningDone()) {
        connector.plan(runId, publisher.getSink());
        publisher.logPlanningDone();
      }
    });
    planner.start();
    PublisherResult result = publisher.waitForCompletion(planner, timeout);

    if (result.getStatus() && !publisher.isHookDone(CoordinatorPublisher.HOOK_FINALIZE_RUN)) {
      result = finalizeRun(connector, publisher, timeout);
    }

    if (!result.getStatus()) {
      return new ConnectorResult(connector, publisher, false, result.getMessage());
    }

    connector.postExecute(runId);
    publisher.logHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE);

    double durationSecs = stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0;
    log.info(String.format("Connector %s feeding to pipeline %s complete: %d units. Time: %.2f secs.",
        connector.getName(), connector.getPipelineName(), publisher.numUnitsDone(), durationSecs));
    return new ConnectorResult(connector, publisher, true, null, durationSecs);
  }

  /**
   * Calls the connector's finalizeRun() and waits for any Documents it publishes to complete.
   */
  private PublisherResult finalizeRun(PartitionableConnector connector, CoordinatorPublisher publisher, int timeout)
      throws Exception {
    CoordinatorThread finalizer = new CoordinatorThread("Finalizer", () -> {
      connector.finalizeRun(runId, publisher);
      publisher.flush();
    });
    finalizer.start();
    finalizer.join();

    PublisherResult result = finalizer.hasException() || publisher.hasPending()
        ? publisher.waitForCompletion(finalizer, timeout)
        : new PublisherResult(true, null);

    if (result.getStatus()) {
      publisher.logHookDone(CoordinatorPublisher.HOOK_FINALIZE_RUN);
    }
    return result;
  }

  private interface CoordinatorTask {
    void run() throws Exception;
  }

  /**
   * Runs one of the Coordinator's tasks for a connector in place of the connector itself, so that the Publisher can
   * watch the task the way it would watch a connector that was publishing.
   */
  private class CoordinatorThread extends ConnectorThread {

    private final CoordinatorTask task;
    private volatile Exception exception;

    CoordinatorThread(String name, CoordinatorTask task) {
      super(null, null, runId, ThreadNameUtils.createName(name, runId));
      this.task = task;
    }

    @Override
    public void run() {
      MDC.put(Document.RUNID_FIELD, runId);
      try {
        task.run();
      } catch (Exception e) {
        exception = e;
      }
    }

    @Override
    public Exception getException() {
      return exception;
    }

    @Override
    public boolean hasException() {
      return exception != null;
    }
  }
}
