package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.message.CoordinatorMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Publisher used by the Coordinator of a distributed crawl, for one connector.
 *
 * It tracks Documents exactly as PublisherImpl does, except that the Documents are published by Crawlers and it
 * learns of each from a CREATE Event. On top of that it tracks work units: which have been dispatched and are not yet
 * done. A connector is complete when planning has finished, no unit is outstanding, and no Document is pending.
 *
 * Everything this class decides is written to the run's Event log before it takes effect, and everything it knows
 * is derived from that log. A Coordinator that replaces one that stopped can therefore rebuild the same state by
 * handling the logged Events again, which is what {@link #recover()} does.
 */
public class CoordinatorPublisher extends PublisherImpl {

  public static final String HOOK_PRE_EXECUTE = "preExecute";
  public static final String HOOK_PREPARE_RUN = "prepareRun";
  public static final String HOOK_FINALIZE_RUN = "finalizeRun";
  public static final String HOOK_POST_EXECUTE = "postExecute";
  private static final Set<String> HOOKS = Set.of(HOOK_PRE_EXECUTE, HOOK_PREPARE_RUN, HOOK_FINALIZE_RUN, HOOK_POST_EXECUTE);

  private static final Logger log = LoggerFactory.getLogger(CoordinatorPublisher.class);

  private final CoordinatorMessenger messenger;
  private final String runId;
  private final String connectorName;
  private final String pipelineName;
  private final String configHash;
  private final int epoch;
  private final int maxOutstandingUnits;
  private final int maxAttempts;

  // Units that have been dispatched and are not done, by unit ID. Holds the most recent dispatch of each.
  private final ConcurrentHashMap<String, WorkUnit> outstandingUnits = new ConcurrentHashMap<>();
  private final Set<String> doneUnits = ConcurrentHashMap.newKeySet();
  private final Set<String> hooksDone = ConcurrentHashMap.newKeySet();
  private volatile boolean planningDone = false;
  private volatile String failure = null;

  // True while handling Events that an earlier Coordinator already acted on. Failures seen then are not acted on again.
  private boolean replaying = false;

  // Signalled whenever a unit completes, for a planner waiting to dispatch.
  private final Object dispatchWindow = new Object();

  private final Counter unitsDispatched;
  private final Counter unitsDone;
  private final Counter unitsFailed;

  public CoordinatorPublisher(Config config, CoordinatorMessenger messenger, String runId, Connector connector,
      String metricsPrefix, int epoch) throws Exception {
    super(config, messenger, runId, connector.getPipelineName(), metricsPrefix, false);
    this.messenger = messenger;
    this.runId = runId;
    this.connectorName = connector.getName();
    this.pipelineName = connector.getPipelineName();
    this.configHash = CrawlConfig.connectorConfigHash(CrawlConfig.connectorConfig(config, connectorName));
    this.epoch = epoch;

    CrawlConfig crawlConfig = new CrawlConfig(config);
    this.maxOutstandingUnits = crawlConfig.maxOutstandingUnits;
    this.maxAttempts = crawlConfig.maxAttempts;

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.unitsDispatched = metrics.counter(metricsPrefix + ".units.dispatched");
    this.unitsDone = metrics.counter(metricsPrefix + ".units.done");
    this.unitsFailed = metrics.counter(metricsPrefix + ".units.failed");
  }

  /**
   * Rebuilds this Publisher's state from the Events already in the run's log. Call before anything else when
   * resuming a run. Afterwards, units that were dispatched and are not done are still outstanding; their Crawlers
   * may have given up on them, so call {@link #redispatchOutstandingUnits()}.
   */
  public void recover() throws Exception {
    replaying = true;
    long numReplayed = 0;

    try {
      while (!messenger.replayComplete()) {
        Event event = messenger.pollEvent();
        if (event != null) {
          handleEvent(event);
          numReplayed++;
        }
      }
    } finally {
      replaying = false;
    }

    log.info("Replayed {} events for connector {}: {} units done, {} outstanding, {} docs pending, planning {}.",
        numReplayed, connectorName, doneUnits.size(), outstandingUnits.size(), numPending(),
        planningDone ? "done" : "not done");
  }

  /**
   * Dispatches every outstanding unit again under this Coordinator's epoch, as a first attempt. Crawlers discard
   * the earlier dispatches, which carry an older epoch.
   */
  public void redispatchOutstandingUnits() throws Exception {
    for (WorkUnit unit : new ArrayList<>(outstandingUnits.values())) {
      logAndDispatch(unit.withEpoch(epoch).withAttempt(1));
    }
  }

  /**
   * Returns the sink a connector's plan() should emit its units to. A unit that this Publisher already knows of,
   * from an earlier Coordinator's planning, is ignored.
   */
  public WorkUnitSink getSink() {
    return (unitKey, payload) -> {
      String unitId = connectorName + "/" + unitKey;
      if (outstandingUnits.containsKey(unitId) || doneUnits.contains(unitId)) {
        return;
      }

      try {
        awaitDispatchWindow();
        logAndDispatch(new WorkUnit(runId, connectorName, pipelineName, unitId, 1, epoch, configHash, payload));
      } catch (ConnectorException e) {
        throw e;
      } catch (Exception e) {
        throw new ConnectorException("Could not dispatch unit " + unitId, e);
      }
    };
  }

  /**
   * Blocks while too much work is in flight: too many units outstanding, or more Documents pending than
   * publisher.maxPendingDocs allows. Crawlers publish without limit, so holding back units is the back-pressure.
   */
  private void awaitDispatchWindow() throws Exception {
    synchronized (dispatchWindow) {
      while (failure == null && (outstandingUnits.size() >= maxOutstandingUnits
          || (getMaxPendingDocs() != null && numPending() >= getMaxPendingDocs()))) {
        dispatchWindow.wait(100);
      }
    }

    if (failure != null) {
      throw new ConnectorException("Planning stopped: " + failure);
    }
  }

  // The unit is logged before it is dispatched. If the Coordinator stops between the two, its successor finds the
  // unit outstanding and dispatches it. In the other order, a unit could be executing that no log knows of.
  private void logAndDispatch(WorkUnit unit) throws Exception {
    outstandingUnits.put(unit.unitId(), unit);
    messenger.sendEvent(new Event(unit.unitId(), runId, unit.toJson(), Event.Type.UNIT_CREATED));
    messenger.dispatchUnit(unit);
    unitsDispatched.inc();
  }

  public void logPlanningDone() throws Exception {
    planningDone = true;
    messenger.sendEvent(new Event(connectorName, runId, null, Event.Type.PLANNING_DONE));
  }

  public boolean isPlanningDone() {
    return planningDone;
  }

  /**
   * Records that a lifecycle method of the connector has returned, so that a Coordinator resuming the run does not
   * call it again.
   */
  public void logHookDone(String hook) throws Exception {
    hooksDone.add(hook);
    messenger.sendEvent(new Event(connectorName, runId, hook, Event.Type.HOOK_DONE));
  }

  public boolean isHookDone(String hook) {
    return hooksDone.contains(hook);
  }

  public int numUnitsDone() {
    return doneUnits.size();
  }

  public int numUnitsOutstanding() {
    return outstandingUnits.size();
  }

  @Override
  protected boolean hasOutstandingWork() {
    return !outstandingUnits.isEmpty();
  }

  @Override
  protected String failureReason() {
    return failure;
  }

  /**
   * Makes waitForCompletion() stop waiting and report the connector as failed, for a reason found outside this class.
   */
  public void fail(String reason) {
    if (failure == null) {
      failure = reason;
    }
  }

  @Override
  public void handleEvent(Event event) {
    // Events arrive over the network. One that is for another run, or that is missing what this method needs, is
    // ignored instead of being allowed to end the run with an exception.
    if (!runId.equals(event.getRunId()) || event.getType() == null || event.getDocumentId() == null) {
      return;
    }

    switch (event.getType()) {
      case UNIT_DONE -> handleUnitDone(event);
      case UNIT_FAILED -> handleUnitFailed(event);
      // The next three record what a Coordinator decided. This Coordinator knows its own decisions, so it only takes
      // them from the log when replaying an earlier Coordinator's. Otherwise they are its own Events coming back.
      case UNIT_CREATED -> {
        if (replaying) {
          handleUnitCreated(event);
        }
      }
      case PLANNING_DONE -> {
        if (replaying && connectorName.equals(event.getDocumentId())) {
          planningDone = true;
        }
      }
      case HOOK_DONE -> {
        if (replaying && connectorName.equals(event.getDocumentId()) && HOOKS.contains(String.valueOf(event.getMessage()))) {
          hooksDone.add(event.getMessage());
        }
      }
      case HEARTBEAT, CANCEL -> { }
      default -> super.handleEvent(event);
    }
  }

  private void handleUnitCreated(Event event) {
    WorkUnit unit;
    try {
      unit = WorkUnit.fromJson(event.getMessage());
    } catch (Exception e) {
      log.error("Ignoring unreadable UNIT_CREATED event for {}.", event.getDocumentId(), e);
      return;
    }

    if (!connectorName.equals(unit.connectorName()) || doneUnits.contains(unit.unitId())) {
      return;
    }

    outstandingUnits.merge(unit.unitId(), unit, (current, logged) -> isNewer(logged, current) ? logged : current);
  }

  private static boolean isNewer(WorkUnit unit, WorkUnit other) {
    return unit.epoch() > other.epoch() || (unit.epoch() == other.epoch() && unit.attempt() > other.attempt());
  }

  /**
   * Returns the outstanding unit the Event reports on, or null if there is none or the Event is about an earlier
   * dispatch of it. A Crawler that was slow to notice its unit had been dispatched again reports on the old one.
   */
  private WorkUnit outstandingUnitFor(Event event, JsonNode message) {
    WorkUnit unit = outstandingUnits.get(event.getDocumentId());

    if (unit == null || unit.attempt() != message.path("attempt").asInt() || unit.epoch() != message.path("epoch").asInt()) {
      log.debug("Ignoring {} for {}: not the current dispatch of an outstanding unit.", event.getType(), event.getDocumentId());
      return null;
    }

    return unit;
  }

  private void handleUnitDone(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null) {
      return;
    }

    doneUnits.add(unit.unitId());
    outstandingUnits.remove(unit.unitId());
    unitsDone.inc();
    log.info("Unit {} done by {}: {} docs in {} ms. {} units outstanding.", unit.unitId(), message.path("crawler").asText(),
        message.path("numPublished").asLong(), message.path("durationMs").asLong(), outstandingUnits.size());

    synchronized (dispatchWindow) {
      dispatchWindow.notifyAll();
    }
  }

  private void handleUnitFailed(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);

    // When replaying, the earlier Coordinator already responded to the failure; if it dispatched the unit again,
    // that is in the log too. Either way the unit stays outstanding and is dispatched again after the replay.
    if (unit == null || replaying) {
      return;
    }

    unitsFailed.inc();
    String error = message.path("error").asText();

    if (unit.attempt() >= maxAttempts) {
      failure = "Unit " + unit.unitId() + " failed after " + unit.attempt() + " attempts: " + error;
      return;
    }

    log.warn("Unit {} failed on attempt {} of {}; dispatching it again. Error: {}", unit.unitId(), unit.attempt(),
        maxAttempts, error);
    try {
      logAndDispatch(unit.withAttempt(unit.attempt() + 1));
    } catch (Exception e) {
      failure = "Unit " + unit.unitId() + " failed and could not be dispatched again: " + e.getMessage();
    }
  }

  private ObjectNode parse(Event event) {
    try {
      return CrawlConfig.parseMessage(event.getMessage());
    } catch (Exception e) {
      log.error("Ignoring unreadable {} event for {}.", event.getType(), event.getDocumentId(), e);
      return null;
    }
  }

  /** Returns the IDs of the units that are currently outstanding. */
  public List<String> outstandingUnitIds() {
    return List.copyOf(outstandingUnits.keySet());
  }
}
