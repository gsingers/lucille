package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.message.CoordinatorMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Publisher used by the Coordinator of a distributed crawl, for one connector.
 *
 * It tracks Documents exactly as PublisherImpl does, except that the Documents are published by Crawlers and it
 * learns of each from a CREATE Event. On top of that it tracks work units: which exist and are not yet done. A
 * connector is complete when planning has finished, no unit is outstanding, and no Document is pending.
 *
 * Units come from two places. The connector's planner emits some. A Crawler executing a unit may hand part of it
 * back, in which case its report names the parts and this class creates a unit for each. Either way this class is
 * the only thing that creates units, so it can tell when it is asked for one it already has.
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

  // fields of the message carried by a unit's Events
  static final String ATTEMPT = "attempt";
  static final String EPOCH = "epoch";
  static final String CRAWLER = "crawler";
  static final String EXECUTION = "execution";
  static final String CHILDREN = "children";
  static final String CHILD_KEY = "key";
  static final String CHILD_PAYLOAD = "payload";
  static final String NUM_CHILDREN = "numChildren";
  static final String NUM_PUBLISHED = "numPublished";
  static final String SOURCE_CALLS = "sourceCalls";
  static final String DURATION_MS = "durationMs";
  static final String ERROR = "error";

  private static final int UNDISPATCHED_EPOCH = 0;

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

  // Units that exist but have not been dispatched, because too much work is in flight. Only the thread that handles
  // Events adds to or takes from the queue; queuedUnitIds is what other threads may read.
  private final ArrayDeque<WorkUnit> queuedUnits = new ArrayDeque<>();
  private final Set<String> queuedUnitIds = ConcurrentHashMap.newKeySet();

  // What executions of outstanding units have handed back so far, by unit ID and then by execution. A unit that is
  // executed twice reports twice, and only the execution whose completion is accepted counts.
  private final Map<String, Map<String, List<JsonNode>>> handedBack = new HashMap<>();

  // True while handling Events that an earlier Coordinator already acted on. Nothing is dispatched then.
  private boolean replaying = false;

  // Signalled whenever a unit completes, for a planner waiting to dispatch.
  private final Object dispatchWindow = new Object();

  // When waitForCompletion() last went round its loop, or 0 when it is not running.
  private volatile long lastWaitIterationMillis = 0;

  private final Counter unitsDispatched;
  private final Counter unitsDone;
  private final Counter unitsFailed;
  private final Counter unitsHandedBack;

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
    this.unitsHandedBack = metrics.counter(metricsPrefix + ".units.handedBack");
  }

  /**
   * Rebuilds this Publisher's state from the Events already in the run's log. Call before anything else when
   * resuming a run. Afterwards, units that exist and are not done are outstanding; their Crawlers may have given
   * up on them, so call {@link #redispatchOutstandingUnits()}.
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
  public void redispatchOutstandingUnits() {
    for (WorkUnit unit : new ArrayList<>(outstandingUnits.values())) {
      enqueue(unit.withEpoch(epoch).withAttempt(1));
      outstandingUnits.remove(unit.unitId());
    }
    dispatchQueuedUnits();
  }

  /**
   * Returns the sink a connector's plan() should emit its units to. A unit that this Publisher already knows of,
   * from an earlier Coordinator's planning or from a Crawler handing it back, is ignored.
   */
  public WorkUnitSink getSink() {
    return (unitKey, payload) -> {
      String unitId = connectorName + "/" + unitKey;
      if (isKnown(unitId)) {
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

  private boolean isKnown(String unitId) {
    return outstandingUnits.containsKey(unitId) || queuedUnitIds.contains(unitId) || doneUnits.contains(unitId);
  }

  // Whether more work may be put in flight: not too many units outstanding, and no more Documents pending than
  // publisher.maxPendingDocs allows. Crawlers publish without limit, so holding back units is the back-pressure.
  private boolean dispatchWindowOpen() {
    return outstandingUnits.size() < maxOutstandingUnits
        && (getMaxPendingDocs() == null || numPending() < getMaxPendingDocs());
  }

  /**
   * Blocks the planner while too much work is in flight.
   */
  private void awaitDispatchWindow() throws Exception {
    synchronized (dispatchWindow) {
      while (failure == null && !dispatchWindowOpen()) {
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
    messenger.logAndDispatchUnit(new Event(unit.unitId(), runId, unit.toJson(), Event.Type.UNIT_CREATED), unit);
    unitsDispatched.inc();
  }

  private void enqueue(WorkUnit unit) {
    queuedUnitIds.add(unit.unitId());
    queuedUnits.add(unit);
  }

  /**
   * Dispatches queued units for as long as more work may be put in flight. The thread that handles Events must not
   * wait for room, since it is the one that makes room, so what cannot be dispatched yet stays queued.
   */
  private void dispatchQueuedUnits() {
    while (failure == null && !queuedUnits.isEmpty() && dispatchWindowOpen()) {
      WorkUnit unit = queuedUnits.poll();
      try {
        logAndDispatch(unit);
      } catch (Exception e) {
        failure = "Unit " + unit.unitId() + " could not be dispatched: " + e.getMessage();
      }
      queuedUnitIds.remove(unit.unitId());
    }
  }

  public void logPlanningDone() throws Exception {
    // every unit has to have been accepted before planning is recorded as done
    messenger.flush();
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

  /** Returns the number of units that exist and are not done, whether or not they have been dispatched. */
  public int numUnitsOutstanding() {
    return outstandingUnits.size() + queuedUnitIds.size();
  }

  @Override
  protected boolean hasOutstandingWork() {
    return !outstandingUnits.isEmpty() || !queuedUnitIds.isEmpty();
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
  public PublisherResult waitForCompletion(ConnectorThread thread, int timeout) throws Exception {
    lastWaitIterationMillis = System.currentTimeMillis();
    try {
      return super.waitForCompletion(thread, timeout);
    } finally {
      lastWaitIterationMillis = 0;
    }
  }

  @Override
  protected void onWaitIteration() {
    lastWaitIterationMillis = System.currentTimeMillis();
    // Documents finishing can reopen the window as well as units finishing, and only the latter is seen as it happens.
    dispatchQueuedUnits();
  }

  /**
   * Returns how long it has been since waitForCompletion() last went round its loop, or 0 if it is not running. The
   * loop goes round every few seconds at most, so a large value means the thread that handles Events is stuck.
   */
  public long millisSinceWaitIteration() {
    long last = lastWaitIterationMillis;
    return last == 0 ? 0 : System.currentTimeMillis() - last;
  }

  @Override
  public void handleEvent(Event event) {
    // Events arrive over the network. One that is for another run, or that is missing what this method needs, is
    // ignored instead of being allowed to end the run with an exception.
    if (!runId.equals(event.getRunId()) || event.getType() == null || event.getDocumentId() == null) {
      return;
    }

    switch (event.getType()) {
      case UNIT_CHILDREN -> handleUnitChildren(event);
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

    if (unit == null || unit.attempt() != message.path(ATTEMPT).asInt() || unit.epoch() != message.path(EPOCH).asInt()) {
      log.debug("Ignoring {} for {}: not the current dispatch of an outstanding unit.", event.getType(), event.getDocumentId());
      return null;
    }

    return unit;
  }

  // A Crawler sends what a unit handed back ahead of the unit's completion, in as many Events as it takes. Nothing is
  // done with it until the completion arrives: an execution that does not complete has handed nothing back.
  private void handleUnitChildren(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null || !message.path(CHILDREN).isArray()) {
      return;
    }

    List<JsonNode> children = handedBack.computeIfAbsent(unit.unitId(), id -> new HashMap<>())
        .computeIfAbsent(message.path(EXECUTION).asText(), execution -> new ArrayList<>());
    message.path(CHILDREN).forEach(children::add);
  }

  private void handleUnitDone(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null) {
      return;
    }

    Map<String, List<JsonNode>> byExecution = handedBack.remove(unit.unitId());
    List<JsonNode> children = byExecution == null ? null : byExecution.get(message.path(EXECUTION).asText());
    int numChildren = children == null ? 0 : children.size();

    // The unit said how much it handed back. If that much has not arrived, creating units for what did arrive
    // would leave part of the source uncrawled without anyone knowing, so the unit is treated as having failed.
    if (numChildren != message.path(NUM_CHILDREN).asInt(0)) {
      failUnit(unit, "Its report named " + message.path(NUM_CHILDREN).asInt(0) + " handed-back parts but "
          + numChildren + " were received.");
      return;
    }

    doneUnits.add(unit.unitId());
    outstandingUnits.remove(unit.unitId());
    unitsDone.inc();

    int numCreated = 0;
    if (children != null) {
      for (JsonNode child : children) {
        numCreated += createHandedBackUnit(child) ? 1 : 0;
      }
    }

    log.info("Unit {} done by {}: {} docs, {} source calls, {} ms, {} handed back. {} units outstanding.", unit.unitId(),
        message.path(CRAWLER).asText(), message.path(NUM_PUBLISHED).asLong(), message.path(SOURCE_CALLS).asLong(),
        message.path(DURATION_MS).asLong(), numCreated, numUnitsOutstanding());

    dispatchQueuedUnits();
    synchronized (dispatchWindow) {
      dispatchWindow.notifyAll();
    }
  }

  /**
   * Creates a unit for a part of the source that a Crawler handed back, unless there already is one. A unit that is
   * executed twice hands the same parts back twice, and the second time they are already known. Returns whether a
   * unit was created.
   */
  private boolean createHandedBackUnit(JsonNode child) {
    if (!child.path(CHILD_KEY).isTextual() || !child.path(CHILD_PAYLOAD).isObject()) {
      log.warn("Ignoring a handed-back part with no key or payload: {}", child);
      return false;
    }

    String unitId = connectorName + "/" + child.path(CHILD_KEY).asText();
    if (isKnown(unitId)) {
      return false;
    }

    // When replaying, nothing is dispatched: the unit is left outstanding, and if the earlier Coordinator got as far
    // as dispatching it, and a Crawler as far as completing it, the log goes on to say so. It is given an epoch
    // lower than any Coordinator's, so that the logged dispatch replaces it and reports on that dispatch are accepted.
    WorkUnit unit = new WorkUnit(runId, connectorName, pipelineName, unitId, 1, replaying ? UNDISPATCHED_EPOCH : epoch,
        configHash, (ObjectNode) child.path(CHILD_PAYLOAD));
    unitsHandedBack.inc();

    if (replaying) {
      outstandingUnits.put(unitId, unit);
    } else {
      enqueue(unit);
    }
    return true;
  }

  private void handleUnitFailed(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit != null) {
      failUnit(unit, message.path(ERROR).asText());
    }
  }

  // Dispatches the unit again, or fails the connector if it has had all its attempts.
  private void failUnit(WorkUnit unit, String error) {
    handedBack.remove(unit.unitId());

    // When replaying, the earlier Coordinator already responded to the failure; if it dispatched the unit again,
    // that is in the log too. Either way the unit stays outstanding and is dispatched again after the replay.
    if (replaying) {
      return;
    }

    unitsFailed.inc();

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

  /** Returns the IDs of the units that exist and are not done, dispatched ones first. */
  public List<String> outstandingUnitIds() {
    List<String> ids = new ArrayList<>(outstandingUnits.keySet());
    ids.addAll(queuedUnitIds);
    return ids;
  }
}
