package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.DistributedCrawlTest.numSucceeded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.ScriptedPartitionedConnector;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.message.IndexerMessengerFactory;
import com.kmwllc.lucille.message.KafkaCoordinatorMessenger;
import com.kmwllc.lucille.message.KafkaCrawlerMessenger;
import com.kmwllc.lucille.message.KafkaRunControl;
import com.kmwllc.lucille.message.KafkaUtils;
import java.util.List;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import com.kmwllc.lucille.message.RunControl;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Runs distributed crawls through an embedded Kafka broker, with Crawlers, Workers and an Indexer as threads of this
 * JVM. Covers what depends on Kafka: delivery of work units, redelivery when a Crawler dies, and a Coordinator
 * resuming a run from its event topic.
 */
public class KafkaDistributedCrawlTest {

  private static EmbeddedKafkaBroker embeddedKafka;

  // Each test uses its own pipeline, and therefore its own topics.
  private static int testNumber = 0;

  private Config config;
  private String pipeline;
  private WorkerPool workerPool;
  private Indexer indexer;
  private Thread indexerThread;
  private CrawlerPool crawlerPool;

  @BeforeClass
  public static void startKafka() {
    embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1);
    embeddedKafka.afterPropertiesSet();
  }

  @AfterClass
  public static void stopKafka() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  @Before
  public void setUp() throws Exception {
    ScriptedPartitionedConnector.reset();
    testNumber++;
    pipeline = "crawl_pipeline" + testNumber;

    config = ConfigFactory.parseString("""
        connectors: [{
          name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
          pipeline: "%1$s", numUnits: 6, docsPerUnit: 3
        }]
        pipelines: [{name: "%1$s", stages: [{class: "com.kmwllc.lucille.stage.NopStage"}]}]
        solr { url: ["http://localhost:null"] }
        indexer { sendEnabled: false, type: "solr" }
        kafka {
          bootstrapServers: "%2$s"
          maxPollIntervalSecs: 600
          consumerGroupId: "lucille_workers_%1$s"
          maxRequestSize: 250000000
        }
        crawl {
          workTopic: "work_%1$s"
          controlTopic: "control_%1$s"
          consumerGroupId: "crawlers_%1$s"
          workTopicPartitions: 4
          threads: 2
          heartbeatSecs: 1
          orphanTimeoutSecs: 3
        }
        """.formatted(pipeline, embeddedKafka.getBrokersAsString()));

    workerPool = new WorkerPool(config, pipeline, null, WorkerMessengerFactory.getKafkaFactory(config, pipeline), pipeline);
    workerPool.start();
    indexer = IndexerFactory.fromConfig(config, IndexerMessengerFactory.getKafkaFactory(config, pipeline).create(), true,
        pipeline);
    indexerThread = new Thread(indexer);
    indexerThread.start();
    crawlerPool = new CrawlerPool(config, CrawlerMessengerFactory.getKafkaFactory(config));
    crawlerPool.start();
  }

  @After
  public void tearDown() throws Exception {
    ScriptedPartitionedConnector.gate.countDown();
    crawlerPool.stop();
    crawlerPool.join(10_000);
    workerPool.stop();
    workerPool.join(10_000);
    indexer.terminate();
    indexerThread.join(10_000);
  }

  private static void waitFor(String description, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 60_000;
    while (!condition.getAsBoolean()) {
      assertTrue("Timed out waiting for " + description, System.currentTimeMillis() < deadline);
      Thread.sleep(100);
    }
  }

  private static boolean executedOnce(String... unitKeys) {
    for (String unitKey : unitKeys) {
      if (ScriptedPartitionedConnector.executionsOf(unitKey) != 1) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testRun() throws Exception {
    RunResult result = Runner.run(config, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    assertTrue(result.getStatus());
    assertEquals(18, numSucceeded(result));
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5"));
    assertEquals(1, ScriptedPartitionedConnector.preExecutes.get());
    assertEquals(1, ScriptedPartitionedConnector.postExecutes.get());

    // the six units were dealt out evenly over the four partitions of the work topic
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createUngroupedConsumer(config, "test", 10)) {
      List<TopicPartition> partitions = consumer.partitionsFor("work_" + pipeline).stream()
          .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
      List<Long> unitsPerPartition = consumer.endOffsets(partitions).values().stream().sorted().toList();
      assertEquals(List.of(1L, 1L, 2L, 2L), unitsPerPartition);
    }

    // the run is over, so its ID cannot be used to start another
    assertFalse(Runner.run(config, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline).getStatus());
  }

  @Test
  public void testUnitThatThrowsAnErrorIsExecutedAgain() throws Exception {
    ScriptedPartitionedConnector.errorOnce.add("u1");
    RunResult result = Runner.run(config, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    // the Error is reported as a failure of the unit instead of silently ending the Crawler's thread
    assertTrue(result.getStatus());
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u1"));
    // The Document that u1 published before the Error may have been indexed as well as its replacement.
    long succeeded = numSucceeded(result);
    assertTrue("succeeded: " + succeeded, succeeded == 18 || succeeded == 19);
  }

  @Test
  public void testUnitOfCrawlerThatDiesIsDeliveredToAnother() throws Exception {
    // the pool's Crawlers would take the unit themselves
    crawlerPool.stop();
    crawlerPool.join(10_000);

    CrawlConfig crawlConfig = new CrawlConfig(config);
    RunControlTracker tracker = new RunControlTracker(60_000);
    WorkUnit unit = new WorkUnit("run1", "connector1", pipeline, "connector1/u0", 1, 1, "hash", WorkUnit.newPayload());
    KafkaCoordinatorMessenger coordinator = new KafkaCoordinatorMessenger(config, false);
    coordinator.initialize("run1", pipeline);
    coordinator.dispatchUnit(unit);
    coordinator.close();

    // one Crawler receives the unit and goes away without acknowledging it
    KafkaCrawlerMessenger first = new KafkaCrawlerMessenger(config, crawlConfig, tracker);
    assertEquals(unit, pollUntilUnit(first));
    first.close();

    // so another receives it
    KafkaCrawlerMessenger second = new KafkaCrawlerMessenger(config, crawlConfig, tracker);
    assertEquals(unit, pollUntilUnit(second));
    second.ackWorkUnit();
    second.close();

    // and once it has been acknowledged, nobody does
    KafkaCrawlerMessenger third = new KafkaCrawlerMessenger(config, crawlConfig, tracker);
    for (int i = 0; i < 5; i++) {
      assertNull(third.pollWorkUnit());
    }
    third.close();
  }

  private static WorkUnit pollUntilUnit(KafkaCrawlerMessenger messenger) throws Exception {
    long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline) {
      WorkUnit unit = messenger.pollWorkUnit();
      if (unit != null) {
        return unit;
      }
    }
    throw new AssertionError("Timed out waiting for a work unit");
  }

  @Test
  public void testFailedUnitIsExecutedAgain() throws Exception {
    ScriptedPartitionedConnector.failOnce.add("u4");
    RunResult result = Runner.run(config, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    assertTrue(result.getStatus());
    assertEquals(18, numSucceeded(result));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u4"));
  }

  /**
   * A RunControl that can be made to go silent, as the control messages of a Coordinator whose process was killed would.
   */
  private static class SilenceableRunControl implements RunControl {

    private final RunControl delegate;
    volatile boolean silent = false;

    SilenceableRunControl(RunControl delegate) {
      this.delegate = delegate;
    }

    @Override
    public Status latest(String runId) throws Exception {
      return delegate.latest(runId);
    }

    @Override
    public void heartbeat(String runId, int epoch, String configHash) throws Exception {
      if (!silent) {
        delegate.heartbeat(runId, epoch, configHash);
      }
    }

    @Override
    public void cancel(String runId, int epoch, String configHash, String reason) throws Exception {
      if (!silent) {
        delegate.cancel(runId, epoch, configHash, reason);
      }
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  @Test
  public void testResumeAfterCoordinatorDies() throws Exception {
    String runId = "run-" + pipeline;

    // only three of the six units can finish until the gate opens
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    ScriptedPartitionedConnector.gateAfter = 3;

    SilenceableRunControl firstControl = new SilenceableRunControl(new KafkaRunControl(config));
    CrawlCoordinator first = new CrawlCoordinator(config, runId, firstControl, () -> new KafkaCoordinatorMessenger(config, false));
    AtomicReference<RunResult> firstResult = new AtomicReference<>();
    Thread firstThread = new Thread(() -> {
      try {
        firstResult.set(first.run(false, false));
      } catch (Exception e) {
        // expected: the thread is interrupted below
      }
    });
    firstThread.start();

    // A run whose Coordinator is alive cannot be taken over by accident.
    waitFor("the first units to be executed", () -> ScriptedPartitionedConnector.completed.size() == 3);
    String[] doneBeforeDeath = ScriptedPartitionedConnector.completed.toArray(new String[0]);
    RunResult refused = new CrawlCoordinator(config, runId, new KafkaRunControl(config),
        () -> new KafkaCoordinatorMessenger(config, true)).run(true, false);
    assertFalse(refused.getStatus());

    // Kill the first Coordinator while half of the units are done and the rest are held up.
    firstControl.silent = true;
    firstThread.interrupt();
    firstThread.join(30_000);
    assertFalse(firstThread.isAlive());

    // Crawlers give up on the run once its heartbeat has been silent for crawl.orphanTimeoutSecs.
    Thread.sleep(4000);
    ScriptedPartitionedConnector.gate.countDown();

    RunResult result = Runner.resumeAndLogResult(config, runId, false, false);

    assertTrue(result.getStatus());
    assertEquals(18, numSucceeded(result));

    // units that were done before the first Coordinator died are not executed again
    assertTrue(executedOnce(doneBeforeDeath));
    // while any that were under way when it died, and were given up, are: one for each Crawler that had joined by then
    assertEquals(6, ScriptedPartitionedConnector.completed.size());
    int executions = ScriptedPartitionedConnector.executions.values().stream().mapToInt(count -> count.get()).sum();
    assertTrue("executions: " + executions, executions == 7 || executions == 8);

    // nor are the lifecycle methods that had already returned
    assertEquals(1, ScriptedPartitionedConnector.preExecutes.get());
    assertEquals(1, ScriptedPartitionedConnector.prepares.get());
    assertEquals(1, ScriptedPartitionedConnector.plans.get());
    assertEquals(1, ScriptedPartitionedConnector.finalizes.get());
    assertEquals(1, ScriptedPartitionedConnector.postExecutes.get());

    // resuming a run that has completed changes nothing
    RunResult again = Runner.resumeAndLogResult(config, runId, false, false);
    assertTrue(again.getStatus());
    assertEquals(1, ScriptedPartitionedConnector.postExecutes.get());
  }
}
