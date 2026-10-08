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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
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
 * Units are dispatched to the partitions of the work topic, one unit in flight per partition. A partition is read
 * by one Crawler thread, which takes its units in order, so a second unit sent to a partition would wait behind the
 * first however many other Crawlers were idle. Instead the units wait here, in a queue ordered by what they are
 * expected to cost, and each goes to whichever partition is next to come free. That way the largest units start
 * first, each on a Crawler of its own, and no unit waits behind a long one, provided there are at least as many
 * Crawler threads as partitions. A thread that reads several partitions takes their units in turn.
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
  public static final String ATTEMPT = "attempt";
  public static final String EPOCH = "epoch";
  public static final String CRAWLER = "crawler";
  static final String EXECUTION = "execution";
  static final String CHILDREN = "children";
  static final String CHILD_KEY = "key";
  static final String CHILD_PAYLOAD = "payload";
  static final String NUM_CHILDREN = "numChildren";
  // on a UNIT_CHILDREN, that the parts were handed back while the unit was executing and may be dispatched at once
  static final String EARLY = "early";
  static final String NUM_PUBLISHED = "numPublished";
  static final String SOURCE_CALLS = "sourceCalls";
  static final String DURATION_MS = "durationMs";
  public static final String ERROR = "error";
  // what kind of failure, and the innermost cause; on a UNIT_DONE, the failure that made the unit hand work back
  public static final String ERROR_CLASS = "errorClass";
  public static final String ERROR_CAUSE = "errorCause";
  public static final String REFUSED_CALLS = "refusedCalls";
  // of the refused calls, those the source throttled; the rest it could not answer
  public static final String THROTTLED_CALLS = "throttledCalls";
  // every request made to the source, failed attempts and fetches included; what refusals are set against
  public static final String REQUESTS = "requests";
  // on a UNIT_DONE: the most pages one directory's listing took, and how long the unit ran after reaching its limit
  public static final String MAX_DIRECTORY_PAGES = "maxDirectoryPages";
  public static final String MILLIS_PAST_BOUND = "millisPastBound";

  private static final int UNDISPATCHED_EPOCH = 0;

  private static final Logger log = LoggerFactory.getLogger(CoordinatorPublisher.class);

  private final CoordinatorMessenger messenger;
  private final String runId;
  private final String connectorName;
  private final String pipelineName;
  private final String configHash;
  private final int epoch;
  private final int maxOutstandingUnits;
  private volatile int maxUnitsInFlight = Integer.MAX_VALUE;
  private final int maxAttempts;
  private final int maxThrottledAttempts;
  private final long throttleBackoffMillis;
  private final long throttleBackoffCapMillis;
  // the time, replaceable by tests that do not want to wait out a backoff
  private LongSupplier clock = System::currentTimeMillis;

  // Planning may emit far more units than can be in flight. They wait here; past this many, the planner waits.
  static final int MAX_QUEUED_UNITS = 100_000;

  // Units that have been dispatched and are not done, by unit ID. Holds the most recent dispatch of each.
  private final ConcurrentHashMap<String, WorkUnit> outstandingUnits = new ConcurrentHashMap<>();
  // The partition each dispatched unit went to, and which partitions have a unit in flight. Guarded by dispatchLock.
  private final Map<String, Integer> unitPartitions = new HashMap<>();
  private final boolean[] partitionBusy;
  // partitions in the order they are tried, and where in that order the last dispatch was
  private final int[] partitionOrder;
  private int partitionCursor = 0;
  private final Set<String> doneUnits = ConcurrentHashMap.newKeySet();
  private final Set<String> hooksDone = ConcurrentHashMap.newKeySet();
  private volatile boolean planningDone = false;
  private volatile String failure = null;

  // Units that exist but have not been dispatched: the most expensive first, and among equals the first queued.
  // Guarded by dispatchLock; queuedUnitIds is what other threads may read.
  private final PriorityQueue<QueuedUnit> queuedUnits = new PriorityQueue<>(
      Comparator.comparingLong(QueuedUnit::cost).reversed().thenComparingLong(QueuedUnit::sequence));
  private final Set<String> queuedUnitIds = ConcurrentHashMap.newKeySet();
  // Of the queued units, those the planner emitted. Unlike handed-back units, which a parent's report names, a
  // planned unit is in the log only once dispatched, so planning is not recorded as done while any is queued.
  private final Set<String> queuedPlannedUnits = ConcurrentHashMap.newKeySet();
  private long nextSequence = 0;
  // What each unit cost in an earlier run of this config, by unit ID: the best guide to what it will cost now.
  private final Map<String, Long> unitCosts;
  // the cost each queued or outstanding unit was given, so that a unit dispatched again keeps its place
  private final ConcurrentHashMap<String, Long> costOf = new ConcurrentHashMap<>();

  // Dispatching is done by the thread that handles Events and by the planner, which must not interleave.
  private final Object dispatchLock = new Object();

  private record QueuedUnit(WorkUnit unit, long cost, long sequence) { }

  // Units waiting out a backoff before they may be queued: the unit, and when. Guarded by dispatchLock.
  private record DelayedUnit(WorkUnit unit, long cost, long dueMillis) { }
  private final List<DelayedUnit> delayedUnits = new ArrayList<>();

  // How many times each unit that is not done has failed, by whether the failure was the source refusing it or
  // not. The two are limited separately: a source that is overloaded is waited out, a unit that is broken is not.
  private final Map<String, Integer> plainFailures = new HashMap<>();
  private final Map<String, Integer> throttledFailures = new HashMap<>();
  // the calls, refusals and throttles each execution of an outstanding unit has reported so far, by unit ID and
  // execution
  private final Map<String, long[]> refusedSoFar = new HashMap<>();

  private record Dispatch(WorkUnit unit, int partition) { }

  // What executions of outstanding units have handed back so far, by unit ID and then by execution. A unit that is
  // executed twice reports twice, and only the execution whose completion is accepted counts.
  private final Map<String, Map<String, Map<String, JsonNode>>> handedBack = new HashMap<>();

  // True while handling Events that an earlier Coordinator already acted on. Nothing is dispatched then.
  private boolean replaying = false;

  // Signalled whenever a unit completes, for a planner waiting for room in the queue.
  private final Object dispatchWindow = new Object();

  // When waitForCompletion() last went round its loop, or 0 when it is not running.
  private volatile long lastWaitIterationMillis = 0;

  private final Counter unitsDispatched;
  private final Counter unitsDone;
  private final Counter unitsFailed;
  private final Counter unitsHandedBack;
  private final Counter unitsThrottled;
  private final Counter callsRefused;
  // the same, for this connector alone; the registry's counters are shared across runs of a JVM
  private final AtomicLong numUnitsThrottled = new AtomicLong();
  private final AtomicLong numCallsRefused = new AtomicLong();
  private final AtomicLong numCallsThrottled = new AtomicLong();
  private final AtomicLong numSourceCalls = new AtomicLong();
  private final AtomicLong numRequests = new AtomicLong();

  /**
   * @param unitCosts what the units cost in an earlier run, by unit ID, or empty. See crawl.costsFromRun.
   */
  public CoordinatorPublisher(Config config, CoordinatorMessenger messenger, String runId, Connector connector,
      String metricsPrefix, int epoch, Map<String, Long> unitCosts) throws Exception {
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
    this.maxThrottledAttempts = crawlConfig.maxThrottledAttempts;
    this.throttleBackoffMillis = TimeUnit.SECONDS.toMillis(crawlConfig.throttleBackoffSecs);
    this.throttleBackoffCapMillis = TimeUnit.SECONDS.toMillis(crawlConfig.throttleBackoffCapSecs);
    if (messenger.numWorkPartitions() < 1) {
      throw new IllegalStateException("The work topic has no partitions; nothing could be dispatched.");
    }
    this.partitionOrder = CrawlConfig.interleavedPartitionOrder(messenger.numWorkPartitions());
    this.partitionBusy = new boolean[partitionOrder.length];
    this.unitCosts = unitCosts;

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.unitsDispatched = metrics.counter(metricsPrefix + ".units.dispatched");
    this.unitsDone = metrics.counter(metricsPrefix + ".units.done");
    this.unitsFailed = metrics.counter(metricsPrefix + ".units.failed");
    this.unitsHandedBack = metrics.counter(metricsPrefix + ".units.handedBack");
    this.unitsThrottled = metrics.counter(metricsPrefix + ".units.throttled");
    this.callsRefused = metrics.counter(metricsPrefix + ".calls.refused");
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
    synchronized (dispatchLock) {
      for (WorkUnit unit : new ArrayList<>(outstandingUnits.values())) {
        outstandingUnits.remove(unit.unitId());
        enqueue(unit.withEpoch(epoch).withAttempt(1), costOf(unit.unitId(), 0));
      }
    }
    dispatchQueuedUnits();
  }

  /**
   * Returns the sink a connector's plan() should emit its units to. A unit that this Publisher already knows of,
   * from an earlier Coordinator's planning or from a Crawler handing it back, is ignored.
   */
  public WorkUnitSink getSink() {
    return new WorkUnitSink() {
      @Override
      public void emit(String unitKey, ObjectNode payload) throws ConnectorException {
        emit(unitKey, payload, 0);
      }

      @Override
      public void emit(String unitKey, ObjectNode payload, long costHint) throws ConnectorException {
        String unitId = connectorName + "/" + unitKey;
        if (isKnown(unitId)) {
          return;
        }

        awaitQueueRoom();
        synchronized (dispatchLock) {
          // looked at again under the lock: a Crawler may have handed the same unit back meanwhile
          if (isKnown(unitId)) {
            return;
          }
          enqueue(new WorkUnit(runId, connectorName, pipelineName, unitId, 1, epoch, configHash, payload),
              costOf(unitId, costHint));
          queuedPlannedUnits.add(unitId);
        }
        dispatchQueuedUnits();
        if (failure != null) {
          throw new ConnectorException("Planning stopped: " + failure);
        }
      }
    };
  }

  // what an earlier run measured for the unit, or failing that what the connector guessed
  private long costOf(String unitId, long hint) {
    return unitCosts.getOrDefault(unitId, hint);
  }

  private boolean isKnown(String unitId) {
    return outstandingUnits.containsKey(unitId) || queuedUnitIds.contains(unitId) || doneUnits.contains(unitId);
  }

  // Whether more work may be put in flight: not too many units outstanding, and no more Documents pending than
  // publisher.maxPendingDocs allows. Crawlers publish without limit, so holding back units is the back-pressure.
  private boolean dispatchWindowOpen() {
    return outstandingUnits.size() < Math.min(maxOutstandingUnits, maxUnitsInFlight)
        && (getMaxPendingDocs() == null || numPending() < getMaxPendingDocs());
  }

  /**
   * Bounds the units in flight by the run's source concurrency: a unit makes at least one call at a time, so no more
   * units than calls. Called by the Coordinator whenever the figure changes; more room is used at once.
   */
  public void setMaxUnitsInFlight(int max) {
    if (max < 1) {
      throw new IllegalArgumentException("At least one unit must be allowed in flight.");
    }
    if (max != maxUnitsInFlight) {
      log.info("Units in flight bounded to {} by the source concurrency.", max);
      maxUnitsInFlight = max;
      dispatchQueuedUnits();
    }
  }

  /**
   * Blocks the planner while the queue is full. Planning is otherwise not held back: a planned unit waits in the
   * queue, a few hundred bytes each, until there is a Crawler to take it.
   */
  private void awaitQueueRoom() throws ConnectorException {
    synchronized (dispatchWindow) {
      while (failure == null && queuedUnitIds.size() >= MAX_QUEUED_UNITS) {
        try {
          dispatchWindow.wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ConnectorException("Interrupted while planning.");
        }
      }
    }

    if (failure != null) {
      throw new ConnectorException("Planning stopped: " + failure);
    }
  }

  // The unit is logged before it is dispatched. If the Coordinator stops between the two, its successor finds the
  // unit outstanding and dispatches it. In the other order, a unit could be executing that no log knows of.
  private void logAndDispatch(Dispatch dispatch) throws Exception {
    WorkUnit unit = dispatch.unit();
    messenger.logAndDispatchUnit(new Event(unit.unitId(), runId, unit.toJson(), Event.Type.UNIT_CREATED), unit,
        dispatch.partition());
    unitsDispatched.inc();
    log.debug("Dispatched unit {} (attempt {}, cost {}) to partition {}.", unit.unitId(), unit.attempt(),
        costOf.getOrDefault(unit.unitId(), 0L), dispatch.partition());
  }

  // Called with dispatchLock held.
  private void enqueue(WorkUnit unit, long cost) {
    queuedUnitIds.add(unit.unitId());
    costOf.put(unit.unitId(), cost);
    queuedUnits.add(new QueuedUnit(unit, cost, nextSequence++));
  }

  // Called with dispatchLock held. A unit with a delay is held aside until it falls due, then queued like any other.
  private void enqueue(WorkUnit unit, long cost, long delayMillis) {
    if (delayMillis <= 0) {
      enqueue(unit, cost);
      return;
    }
    queuedUnitIds.add(unit.unitId());
    costOf.put(unit.unitId(), cost);
    delayedUnits.add(new DelayedUnit(unit, cost, clock.getAsLong() + delayMillis));
  }

  // Called with dispatchLock held.
  private void queueDueUnits() {
    long now = clock.getAsLong();
    for (var it = delayedUnits.iterator(); it.hasNext(); ) {
      DelayedUnit delayed = it.next();
      if (delayed.dueMillis() <= now) {
        it.remove();
        queuedUnits.add(new QueuedUnit(delayed.unit(), delayed.cost(), nextSequence++));
      }
    }
  }

  /** Returns the number of units waiting out a backoff before they are dispatched again. */
  int numUnitsDelayed() {
    synchronized (dispatchLock) {
      return delayedUnits.size();
    }
  }

  // package access for unit tests
  void setClock(LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * Returns the next partition with no unit in flight, or -1 if every partition has one. Partitions are tried in
   * an order that reaches different Crawlers in turn, carrying on from wherever the last dispatch went.
   */
  private int nextFreePartition() {
    for (int i = 0; i < partitionOrder.length; i++) {
      int partition = partitionOrder[(partitionCursor + i) % partitionOrder.length];
      if (!partitionBusy[partition]) {
        partitionCursor = (partitionCursor + i + 1) % partitionOrder.length;
        return partition;
      }
    }
    return -1;
  }

  // A unit that has been reported on, by a Crawler or by the messenger, no longer has a partition to itself.
  private void freePartitionOf(String unitId) {
    synchronized (dispatchLock) {
      Integer partition = unitPartitions.remove(unitId);
      if (partition != null) {
        partitionBusy[partition] = false;
      }
    }
  }

  /**
   * Dispatches queued units, most expensive first, for as long as there is a free partition and more work may be
   * put in flight. The thread that handles Events must not wait for room, since it is the one that makes room, so
   * what cannot be dispatched yet stays queued.
   *
   * The units and partitions are taken under the lock, and the sends are made without it: a send can block for as
   * long as the producer's max.block.ms when Kafka is unreachable, and the thread that handles Events must not be
   * kept from freeing partitions for that long, or the Coordinator looks stuck.
   */
  private void dispatchQueuedUnits() {
    List<Dispatch> ready = new ArrayList<>();

    synchronized (dispatchLock) {
      queueDueUnits();
      while (failure == null && !queuedUnits.isEmpty() && dispatchWindowOpen()) {
        int partition = nextFreePartition();
        if (partition < 0) {
          break;
        }

        WorkUnit unit = queuedUnits.poll().unit();
        queuedUnitIds.remove(unit.unitId());
        queuedPlannedUnits.remove(unit.unitId());
        outstandingUnits.put(unit.unitId(), unit);
        unitPartitions.put(unit.unitId(), partition);
        partitionBusy[partition] = true;
        ready.add(new Dispatch(unit, partition));
      }
    }

    for (Dispatch dispatch : ready) {
      try {
        logAndDispatch(dispatch);
      } catch (Exception e) {
        failure = "Unit " + dispatch.unit().unitId() + " could not be dispatched: " + e.getMessage();
      }
    }
  }

  /** Returns how many partitions the work topic has: the most units that can be in flight. */
  public int numPartitions() {
    return partitionOrder.length;
  }

  /** Returns how many partitions have a unit in flight. */
  int numPartitionsBusy() {
    synchronized (dispatchLock) {
      int busy = 0;
      for (boolean b : partitionBusy) {
        busy += b ? 1 : 0;
      }
      return busy;
    }
  }

  /**
   * Records that planning has finished, once every planned unit is in the log. A planned unit is logged when it is
   * dispatched, and dispatch waits for a partition, so this waits for the planned units still queued. If the
   * Coordinator dies first, planning is not on record as done and its successor plans again; the units it comes to
   * that are already known are ignored.
   */
  public void logPlanningDone() throws Exception {
    synchronized (dispatchWindow) {
      while (failure == null && !queuedPlannedUnits.isEmpty()) {
        dispatchWindow.wait(100);
      }
    }
    if (failure != null) {
      throw new ConnectorException("Planning stopped: " + failure);
    }

    // every unit has to have been accepted before planning is recorded as done
    messenger.flush();
    planningDone = true;
    messenger.sendEvent(new Event(connectorName, runId, null, Event.Type.PLANNING_DONE));
  }

  /** Returns the number of planned units waiting to be dispatched. */
  int numPlannedUnitsQueued() {
    return queuedPlannedUnits.size();
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
      case UNIT_PROGRESS -> handleUnitProgress(event);
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

  // A Crawler sends what a unit handed back ahead of the unit's completion, in as many Events as it takes. Most of
  // it waits for the completion: an execution that does not complete has handed nothing back. Parts marked early
  // were handed back while the unit was still executing, and are made units at once, so that the rest of the run
  // need not wait for a long unit to finish; if the unit then fails, they stand, and the unit is executed again whole.
  // They are counted with the rest when the completion arrives.
  private void handleUnitChildren(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null || !message.path(CHILDREN).isArray()) {
      return;
    }

    // by key: a batch sent again, after a send that failed only as far as the Crawler could tell, is not counted twice
    Map<String, JsonNode> children = handedBack.computeIfAbsent(unit.unitId(), id -> new HashMap<>())
        .computeIfAbsent(message.path(EXECUTION).asText(), execution -> new LinkedHashMap<>());
    message.path(CHILDREN).forEach(child -> children.putIfAbsent(
        child.path(CHILD_KEY).isTextual() ? child.path(CHILD_KEY).asText() : child.toString(), child));

    if (message.path(EARLY).asBoolean(false)) {
      int numCreated = 0;
      synchronized (dispatchLock) {
        for (JsonNode child : message.path(CHILDREN)) {
          numCreated += createHandedBackUnit(child, 0, 0) ? 1 : 0;
        }
      }
      log.info("Unit {} handed back {} parts while executing.", unit.unitId(), numCreated);
      dispatchQueuedUnits();
    }
  }

  // A unit's calls and refusals arrive as running totals, in progress reports and then in its completion; only what
  // is new since the last report is counted, so that each is counted once. What an earlier Coordinator saw is its own
  // business: nothing is counted while replaying. A report that does not say how many refusals were throttles, from
  // a Crawler that does not tell them apart, is taken to mean all of them.
  private void countRefusedSoFar(WorkUnit unit, ObjectNode message) {
    if (replaying) {
      return;
    }
    String key = unit.unitId() + "#" + message.path(EXECUTION).asText();
    long refused = message.path(REFUSED_CALLS).asLong(0);
    long[] total = {message.path(SOURCE_CALLS).asLong(0), refused, message.path(THROTTLED_CALLS).asLong(refused),
        message.path(REQUESTS).asLong(0)};
    long[] before = refusedSoFar.put(key, total);
    long[] delta = new long[total.length];
    for (int i = 0; i < total.length; i++) {
      delta[i] = Math.max(0, total[i] - (before == null ? 0 : before[i]));
    }
    numSourceCalls.addAndGet(delta[0]);
    countRefused(delta[1]);
    numCallsThrottled.addAndGet(delta[2]);
    numRequests.addAndGet(delta[3]);
  }

  private void handleUnitProgress(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null || replaying) {
      return;
    }
    countRefusedSoFar(unit, message);
    log.debug("Unit {} in progress: {} source calls, {} refused, {} docs.", unit.unitId(),
        message.path(SOURCE_CALLS).asLong(), message.path(REFUSED_CALLS).asLong(), message.path(NUM_PUBLISHED).asLong());
  }

  private void handleUnitDone(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit == null) {
      return;
    }

    Map<String, Map<String, JsonNode>> byExecution = handedBack.remove(unit.unitId());
    Map<String, JsonNode> received = byExecution == null ? null : byExecution.get(message.path(EXECUTION).asText());
    List<JsonNode> children = received == null ? null : new ArrayList<>(received.values());
    int numChildren = children == null ? 0 : children.size();

    // The unit said how much it handed back. If that much has not arrived, creating units for what did arrive
    // would leave part of the source uncrawled without anyone knowing, so the unit is treated as having failed.
    if (numChildren != message.path(NUM_CHILDREN).asInt(0)) {
      failUnit(unit, "Its report named " + message.path(NUM_CHILDREN).asInt(0) + " handed-back parts but "
          + numChildren + " were received.");
      return;
    }

    // A unit that could not list the very directory it was given hands that directory back, under its own key. It
    // has done nothing that counts, so it is not done: it is a unit whose source refused it, and is treated as one.
    FailureClass endedBy = FailureClass.parse(message.path(ERROR_CLASS).asText(null));
    if (children != null && children.stream().anyMatch(child -> unit.unitId().equals(childUnitId(child)))) {
      countRefusedSoFar(unit, message);
      refusedSoFar.keySet().removeIf(key -> key.startsWith(unit.unitId() + "#"));
      log.info("Unit {} handed itself back ({}: {}); treating it as a failure of the source.", unit.unitId(), endedBy,
          message.path(ERROR_CAUSE).asText());
      failUnit(unit, "Could not list its own directory: " + message.path(ERROR_CAUSE).asText(),
          endedBy != null && endedBy.isSourceFailure() ? endedBy : FailureClass.SOURCE_ERROR);
      return;
    }

    doneUnits.add(unit.unitId());
    outstandingUnits.remove(unit.unitId());
    costOf.remove(unit.unitId());
    plainFailures.remove(unit.unitId());
    freePartitionOf(unit.unitId());
    unitsDone.inc();
    countRefusedSoFar(unit, message);
    refusedSoFar.keySet().removeIf(key -> key.startsWith(unit.unitId() + "#"));

    // A unit that completed because its source refused the rest hands that rest back. It is not sent straight back
    // to the same source; it waits as a failed unit would, and the parts carry the count of refusals that led to
    // them, so that a part refused in its turn waits longer, and a subtree the source will never serve ends the run
    // at crawl.maxThrottledAttempts instead of being handed back for ever.
    // A unit that completed whole, whatever it was refused before, passes nothing on: its parts are not under suspicion.
    int throttles = endedBy != null && endedBy.isOverload() ? throttledFailures.getOrDefault(unit.unitId(), 0) + 1 : 0;
    throttledFailures.remove(unit.unitId());
    if (!replaying && throttles >= maxThrottledAttempts && children != null && !children.isEmpty()) {
      failure = "Unit " + unit.unitId() + " and what it came from were refused by the source " + throttles + " times: "
          + message.path(ERROR_CAUSE).asText();
      return;
    }
    long childDelayMillis = throttles > 0 && !replaying ? backoffMillis(throttles) : 0;

    int numCreated = 0;
    synchronized (dispatchLock) {
      if (children != null) {
        for (JsonNode child : children) {
          numCreated += createHandedBackUnit(child, childDelayMillis, throttles) ? 1 : 0;
        }
      }
    }

    log.info("Unit {} done by {}: {} docs, {} source calls, {} refused, {} ms, {} handed back{}{}. {} units outstanding, {} queued.",
        unit.unitId(), message.path(CRAWLER).asText(), message.path(NUM_PUBLISHED).asLong(),
        message.path(SOURCE_CALLS).asLong(), message.path(REFUSED_CALLS).asLong(0), message.path(DURATION_MS).asLong(),
        numCreated, endedBy == null ? "" : " after " + endedBy + " (" + message.path(ERROR_CAUSE).asText() + ")",
        serialFloor(message), outstandingUnits.size(), queuedUnitIds.size());

    dispatchQueuedUnits();
    synchronized (dispatchWindow) {
      dispatchWindow.notifyAll();
    }
  }

  // What bounds the unit from below, where the Crawler said: the longest single listing, which no number of Crawlers
  // shortens, and how long the unit went on after reaching its limit.
  static String serialFloor(JsonNode message) {
    StringBuilder floor = new StringBuilder();
    if (message.has(MAX_DIRECTORY_PAGES)) {
      floor.append(", longest listing ").append(message.path(MAX_DIRECTORY_PAGES).asLong()).append(" pages");
    }
    if (message.has(MILLIS_PAST_BOUND)) {
      floor.append(", ").append(message.path(MILLIS_PAST_BOUND).asLong()).append(" ms past its limit");
    }
    return floor.toString();
  }

  /**
   * Creates a unit for a part of the source that a Crawler handed back, unless there already is one. A unit that is
   * executed twice hands the same parts back twice, and the second time they are already known. Returns whether a
   * unit was created.
   */
  private String childUnitId(JsonNode child) {
    return child.path(CHILD_KEY).isTextual() ? connectorName + "/" + child.path(CHILD_KEY).asText() : null;
  }

  private boolean createHandedBackUnit(JsonNode child, long delayMillis, int inheritedThrottles) {
    if (!child.path(CHILD_KEY).isTextual() || !child.path(CHILD_PAYLOAD).isObject()) {
      log.warn("Ignoring a handed-back part with no key or payload: {}", child);
      return false;
    }

    String unitId = childUnitId(child);
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
      if (inheritedThrottles > 0) {
        throttledFailures.put(unitId, inheritedThrottles);
      }
      enqueue(unit, costOf(unitId, 0), delayMillis);
    }
    return true;
  }

  private void handleUnitFailed(Event event) {
    ObjectNode message = parse(event);
    WorkUnit unit = message == null ? null : outstandingUnitFor(event, message);
    if (unit != null) {
      countRefusedSoFar(unit, message);
      refusedSoFar.keySet().removeIf(key -> key.startsWith(unit.unitId() + "#"));
      failUnit(unit, message.path(ERROR).asText(), FailureClass.parse(message.path(ERROR_CLASS).asText(null)));
    }
  }

  private void failUnit(WorkUnit unit, String error) {
    failUnit(unit, error, FailureClass.CONNECTOR_ERROR);
  }

  /**
   * Queues the unit to be dispatched again, or fails the connector if the unit has had all its attempts. A unit
   * whose source refused it is not dispatched straight back into the same overload: it waits, longer each time, and
   * such failures are counted against crawl.maxThrottledAttempts rather than crawl.maxAttempts.
   */
  private void failUnit(WorkUnit unit, String error, FailureClass failureClass) {
    handedBack.remove(unit.unitId());

    // When replaying, the earlier Coordinator already responded to the failure; if it dispatched the unit again,
    // that is in the log too. Either way the unit stays outstanding and is dispatched again after the replay.
    if (replaying) {
      return;
    }

    unitsFailed.inc();
    freePartitionOf(unit.unitId());
    boolean throttled = failureClass != null && failureClass.isOverload();
    Map<String, Integer> failures = throttled ? throttledFailures : plainFailures;
    int count = failures.merge(unit.unitId(), 1, Integer::sum);
    int limit = throttled ? maxThrottledAttempts : maxAttempts;

    if (count >= limit) {
      failure = "Unit " + unit.unitId() + (throttled ? " failed " + count + " times because its source refused it: "
          : " failed after " + count + " attempts: ") + error;
      return;
    }

    long delayMillis = throttled ? backoffMillis(count) : 0;
    if (throttled) {
      unitsThrottled.inc();
      numUnitsThrottled.incrementAndGet();
    }
    log.warn("Unit {} failed ({}, {} of {}); dispatching it again{}. Error: {}", unit.unitId(),
        failureClass == null ? "unclassified" : failureClass, count, limit,
        delayMillis > 0 ? " in " + delayMillis + " ms" : "", error);
    synchronized (dispatchLock) {
      outstandingUnits.remove(unit.unitId());
      enqueue(unit.withAttempt(unit.attempt() + 1), costOf.getOrDefault(unit.unitId(), 0L), delayMillis);
    }
    dispatchQueuedUnits();
  }

  // How long a unit waits after its nth failure at the hands of its source: doubling, capped, jittered.
  private long backoffMillis(int failures) {
    long ceiling = Math.min(throttleBackoffCapMillis, throttleBackoffMillis << Math.min(failures - 1, 20));
    return ThreadLocalRandom.current().nextLong(ceiling / 2 + 1, ceiling + 1);
  }

  private void countRefused(long calls) {
    callsRefused.inc(calls);
    numCallsRefused.addAndGet(calls);
  }

  /** Returns how many calls to the source the Crawlers have reported as refused, over the whole connector. */
  public long numCallsRefused() {
    return numCallsRefused.get();
  }

  /** Returns how many of the refused calls the source throttled; the rest it could not answer. */
  public long numCallsThrottled() {
    return numCallsThrottled.get();
  }

  /** Returns how many calls to the source the Crawlers have reported making, over the whole connector. */
  public long numSourceCalls() {
    return numSourceCalls.get();
  }

  /** Returns how many requests to the source the Crawlers have reported, or 0 if their connector does not count them. */
  public long numRequests() {
    return numRequests.get();
  }

  /** Returns how many times a unit was dispatched again after its source refused it. */
  public long numUnitsThrottled() {
    return numUnitsThrottled.get();
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
