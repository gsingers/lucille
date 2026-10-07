package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FailureClass;
import com.kmwllc.lucille.core.SourceException;
import com.kmwllc.lucille.core.PartitionableConnector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.UnitContext;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.core.WorkUnitSink;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A partitioned connector for tests of the distributed crawl. Plans <code>numUnits</code> units of
 * <code>docsPerUnit</code> Documents each, records how often each lifecycle method and each unit is executed, and
 * can be told to misbehave when executing particular units.
 *
 * All state is static, since the Coordinator and each Crawler build their own instances. Call reset() before each test.
 */
public class ScriptedPartitionedConnector extends AbstractConnector implements PartitionableConnector {

  public static final Spec SPEC = SpecBuilder.connector().requiredNumber("numUnits", "docsPerUnit").build();

  public static final AtomicInteger preExecutes = new AtomicInteger();
  public static final AtomicInteger prepares = new AtomicInteger();
  public static final AtomicInteger plans = new AtomicInteger();
  public static final AtomicInteger finalizes = new AtomicInteger();
  public static final AtomicInteger postExecutes = new AtomicInteger();
  public static final Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();

  /** Units that throw an exception the first time they are executed. */
  public static final Set<String> failOnce = ConcurrentHashMap.newKeySet();
  /** Units that throw an exception every time they are executed. */
  public static final Set<String> failAlways = ConcurrentHashMap.newKeySet();
  /** Units that, the first time they are executed, throw an Error after publishing one Document. */
  public static final Set<String> errorOnce = ConcurrentHashMap.newKeySet();
  /**
   * Once this many units have been started, each unit started after them waits for {@link #gate} to open before
   * publishing anything. Negative for no limit. Counted across units rather than naming them, because which units
   * a Crawler receives first depends on how they were spread over the work topic.
   */
  public static volatile int gateAfter = -1;
  public static volatile CountDownLatch gate = new CountDownLatch(0);
  private static final AtomicInteger started = new AtomicInteger();
  /** Units that wait for {@link #gate} to open before publishing anything, whenever they are executed. */
  public static final Set<String> gatedUnits = ConcurrentHashMap.newKeySet();
  /** Units that are waiting for {@link #gate} to open, or have waited for it. */
  public static final Set<String> reachedGate = ConcurrentHashMap.newKeySet();
  /** Units that have published all of their Documents. */
  public static final Set<String> completed = ConcurrentHashMap.newKeySet();
  /**
   * For a unit, the keys of the parts it hands back each time it is executed. A part is executed like a planned unit,
   * and may itself have parts to hand back.
   */
  public static final Map<String, List<String>> handBacks = new ConcurrentHashMap<>();
  /**
   * For a unit, the keys of parts it hands back first thing, after which it waits, for up to {@link #earlyWaitMillis},
   * until they have all completed. Records in {@link #outlivedEarlyParts} whether they did.
   */
  public static final Map<String, List<String>> earlyHandBacks = new ConcurrentHashMap<>();
  public static final Set<String> outlivedEarlyParts = ConcurrentHashMap.newKeySet();
  public static volatile long earlyWaitMillis = 10_000;
  /** Of those units, the ones that meet a throttled source first, so that what they hand back is the source's refusal. */
  public static final Set<String> earlyAfterSourceError = ConcurrentHashMap.newKeySet();
  /** Units that, the first time they are executed, wait for {@link #hang} to open before doing anything. */
  public static final Set<String> hangOnce = ConcurrentHashMap.newKeySet();
  /** Units that found their execution cancelled once {@link #hang} had opened. */
  public static final Set<String> sawCancelled = ConcurrentHashMap.newKeySet();
  /**
   * Units that, the first time they are executed, publish half of their Documents, then meet a throttled source:
   * they hand back a part named after them, record the failure, and complete.
   */
  public static final Set<String> throttledOnce = ConcurrentHashMap.newKeySet();
  /** Units that, the first time they are executed, fail with a throttling failure; or the first two times; or always. */
  public static final Set<String> throttleFailOnce = ConcurrentHashMap.newKeySet();
  public static final Set<String> throttleFailTwice = ConcurrentHashMap.newKeySet();
  public static final Set<String> throttleFailAlways = ConcurrentHashMap.newKeySet();
  /** The most executions that were under way at once. */
  public static final AtomicInteger maxConcurrentExecutions = new AtomicInteger();
  private static final AtomicInteger executing = new AtomicInteger();
  /** The source allowance each execution saw, in order. */
  public static final List<Integer> allowancesSeen = Collections.synchronizedList(new ArrayList<>());
  /**
   * A source that takes only so many concurrent calls. When set to a positive limit, each execution of a unit makes
   * as many concurrent calls as its allowance says (or 50 without one), for {@link #sourceHoldMillis}; calls beyond
   * the limit are refused and counted on the context.
   */
  public static volatile int sourceLimit = 0;
  public static volatile long sourceHoldMillis = 200;
  private static final AtomicInteger sourceInFlight = new AtomicInteger();
  public static final AtomicLong sourceRefused = new AtomicLong();
  /** Units that wait for {@link #hang} to open every time they are executed. */
  public static final Set<String> hangAlways = ConcurrentHashMap.newKeySet();
  public static volatile CountDownLatch hang = new CountDownLatch(0);
  /** Published by finalizeRun(), if set. */
  public static volatile String finalizeDocId = null;

  private final int numUnits;
  private final int docsPerUnit;

  public ScriptedPartitionedConnector(Config config) {
    super(config);
    this.numUnits = config.getInt("numUnits");
    this.docsPerUnit = config.getInt("docsPerUnit");
  }

  public static void reset() {
    for (AtomicInteger counter : new AtomicInteger[] {preExecutes, prepares, plans, finalizes, postExecutes}) {
      counter.set(0);
    }
    executions.clear();
    failOnce.clear();
    failAlways.clear();
    errorOnce.clear();
    gateAfter = -1;
    gate = new CountDownLatch(0);
    started.set(0);
    completed.clear();
    handBacks.clear();
    earlyHandBacks.clear();
    outlivedEarlyParts.clear();
    earlyWaitMillis = 10_000;
    earlyAfterSourceError.clear();
    hangOnce.clear();
    throttledOnce.clear();
    throttleFailOnce.clear();
    allowancesSeen.clear();
    maxConcurrentExecutions.set(0);
    executing.set(0);
    sourceLimit = 0;
    sourceHoldMillis = 200;
    sourceInFlight.set(0);
    sourceRefused.set(0);
    throttleFailTwice.clear();
    throttleFailAlways.clear();
    sawCancelled.clear();
    gatedUnits.clear();
    reachedGate.clear();
    hangAlways.clear();
    hang.countDown();
    hang = new CountDownLatch(0);
    finalizeDocId = null;
  }

  public static int executionsOf(String unitKey) {
    AtomicInteger count = executions.get(unitKey);
    return count == null ? 0 : count.get();
  }

  @Override
  public void preExecute(String runId) {
    preExecutes.incrementAndGet();
  }

  @Override
  public void prepareRun(String runId) {
    prepares.incrementAndGet();
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("execute() should not be called in a distributed crawl.");
  }

  @Override
  public void plan(String runId, WorkUnitSink sink) throws ConnectorException {
    plans.incrementAndGet();
    for (int i = 0; i < numUnits; i++) {
      sink.emit("u" + i, WorkUnit.newPayload().put("unit", i));
    }
  }

  @Override
  public void executeUnit(WorkUnit unit, Publisher publisher, UnitContext context) throws ConnectorException {
    // a planned unit is described by its number, a handed-back part by its key
    String unitKey = unit.payload().has("key") ? unit.payload().get("key").asText() : "u" + unit.payload().get("unit").asInt();
    int execution = executions.computeIfAbsent(unitKey, k -> new AtomicInteger()).incrementAndGet();

    if (failAlways.contains(unitKey) || (execution == 1 && failOnce.contains(unitKey))) {
      throw new ConnectorException("Scripted failure of " + unitKey);
    }
    if (throttleFailAlways.contains(unitKey) || (execution <= 2 && throttleFailTwice.contains(unitKey))
        || (execution == 1 && throttleFailOnce.contains(unitKey))) {
      throw new ConnectorException("Error occurred while traversing " + unitKey,
          new SourceException(FailureClass.THROTTLED, "Could not list " + unitKey, new RuntimeException("429 Too Many Requests")));
    }

    allowancesSeen.add(context.maxSourceConcurrency());
    maxConcurrentExecutions.accumulateAndGet(executing.incrementAndGet(), Math::max);

    try {
      if (sourceLimit > 0) {
        callTheSource(context);
      }
      if (execution == 1 && earlyHandBacks.containsKey(unitKey)) {
        awaitEarlyParts(earlyHandBacks.get(unitKey), context, unitKey);
      }
      if (hangAlways.contains(unitKey) || (execution == 1 && hangOnce.contains(unitKey))) {
        hang.await();
        if (context.isCancelled()) {
          sawCancelled.add(unitKey);
        }
      }
      if ((started.incrementAndGet() > gateAfter && gateAfter >= 0) || gatedUnits.contains(unitKey)) {
        reachedGate.add(unitKey);
        gate.await();
      }

      for (int i = 0; i < docsPerUnit; i++) {
        if (execution == 1 && throttledOnce.contains(unitKey) && i == docsPerUnit / 2) {
          // the source refused the rest: handed back under a key of its own, to be executed as a unit
          context.recordSourceError(FailureClass.THROTTLED, "RuntimeException: 429 Too Many Requests");
          context.handBack(unitKey + "-rest", WorkUnit.newPayload().put("key", unitKey + "-rest"));
          context.addRefusedCalls(3);
          break;
        }
        publisher.publish(Document.create(createDocId(unitKey + "-" + i)));

        if (execution == 1 && errorOnce.contains(unitKey)) {
          // an Error rather than an Exception, as a missing class or a stack overflow in a connector would be
          throw new NoClassDefFoundError("Scripted error while executing " + unitKey);
        }
      }
      for (String part : handBacks.getOrDefault(unitKey, List.of())) {
        context.handBack(part, WorkUnit.newPayload().put("key", part));
      }
      context.addSourceCalls(1);
      completed.add(unitKey);
    } catch (Exception e) {
      throw new ConnectorException("Error publishing document", e);
    } finally {
      executing.decrementAndGet();
    }
  }

  private static void awaitEarlyParts(List<String> parts, UnitContext context, String unitKey) throws InterruptedException {
    if (earlyAfterSourceError.contains(unitKey)) {
      context.recordSourceError(FailureClass.THROTTLED, "RuntimeException: 429 Too Many Requests");
    }
    for (String part : parts) {
      context.handBack(part, WorkUnit.newPayload().put("key", part));
    }
    long deadline = System.currentTimeMillis() + earlyWaitMillis;
    while (System.currentTimeMillis() < deadline && !completed.containsAll(parts)) {
      Thread.sleep(50);
    }
    if (completed.containsAll(parts)) {
      outlivedEarlyParts.add(unitKey);
    }
  }

  // Makes the unit's share of concurrent calls against the limited source: those beyond the limit are refused.
  private static void callTheSource(UnitContext context) throws InterruptedException {
    int calls = context.maxSourceConcurrency() == null ? 50 : context.maxSourceConcurrency();
    int inFlight = sourceInFlight.addAndGet(calls);
    try {
      int refused = Math.max(0, Math.min(calls, inFlight - sourceLimit));
      if (refused > 0) {
        sourceRefused.addAndGet(refused);
        context.addRefusedCalls(refused);
      }
      Thread.sleep(sourceHoldMillis);
    } finally {
      sourceInFlight.addAndGet(-calls);
    }
  }

  @Override
  public void finalizeRun(String runId, Publisher publisher) throws ConnectorException {
    finalizes.incrementAndGet();

    if (finalizeDocId != null) {
      try {
        publisher.publish(Document.create(finalizeDocId));
      } catch (Exception e) {
        throw new ConnectorException("Error publishing document", e);
      }
    }
  }

  @Override
  public void postExecute(String runId) {
    postExecutes.incrementAndGet();
  }
}
