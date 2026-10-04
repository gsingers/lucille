package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PartitionableConnector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.core.WorkUnitSink;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
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
  /** Units that have published all of their Documents. */
  public static final Set<String> completed = ConcurrentHashMap.newKeySet();
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
  public void executeUnit(WorkUnit unit, Publisher publisher) throws ConnectorException {
    int unitNumber = unit.payload().get("unit").asInt();
    String unitKey = "u" + unitNumber;
    int execution = executions.computeIfAbsent(unitKey, k -> new AtomicInteger()).incrementAndGet();

    if (failAlways.contains(unitKey) || (execution == 1 && failOnce.contains(unitKey))) {
      throw new ConnectorException("Scripted failure of " + unitKey);
    }

    try {
      if (started.incrementAndGet() > gateAfter && gateAfter >= 0) {
        gate.await();
      }

      for (int i = 0; i < docsPerUnit; i++) {
        publisher.publish(Document.create(createDocId(unitKey + "-" + i)));

        if (execution == 1 && errorOnce.contains(unitKey)) {
          // an Error rather than an Exception, as a missing class or a stack overflow in a connector would be
          throw new NoClassDefFoundError("Scripted error while executing " + unitKey);
        }
      }
      completed.add(unitKey);
    } catch (Exception e) {
      throw new ConnectorException("Error publishing document", e);
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
