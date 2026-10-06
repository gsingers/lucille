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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import com.kmwllc.lucille.message.RunControl;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
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
    ScriptedPartitionedConnector.hang.countDown();
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
    coordinator.dispatchUnit(unit, 0);
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
  public void testStartOrResume() throws Exception {
    String runId = "run-" + pipeline;

    // there is no record of the run, so it is started
    RunResult result = Runner.startOrResumeAndLogResult(config, runId, false);
    assertTrue(result.getStatus());
    assertEquals(18, numSucceeded(result));
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5"));

    // Now there is, so it is resumed. The run's Events show that it completed, so nothing is done again.
    RunResult again = Runner.startOrResumeAndLogResult(config, runId, false);
    assertTrue(again.getStatus());
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5"));
    assertEquals(1, ScriptedPartitionedConnector.preExecutes.get());
    assertEquals(1, ScriptedPartitionedConnector.postExecutes.get());

    KafkaRunControl runControl = new KafkaRunControl(config);
    try {
      Map<String, RunControl.Status> runs = runControl.list();
      assertEquals(java.util.Set.of(runId), runs.keySet());
      assertTrue(runs.get(runId).cancelled());
      assertEquals("complete", runs.get(runId).reason());
      assertEquals(2, runs.get(runId).epoch());
    } finally {
      runControl.close();
    }
  }

  @Test
  public void testHandedBackPartsAreExecutedAsUnits() throws Exception {
    String runId = "run-" + pipeline;
    ScriptedPartitionedConnector.handBacks.put("u0", List.of("p1", "p2"));
    ScriptedPartitionedConnector.handBacks.put("p1", List.of("p3"));
    // a part that another unit hands back as well, and a unit the planner made
    ScriptedPartitionedConnector.handBacks.put("u4", List.of("p2", "u5"));

    RunResult result = Runner.startOrResumeAndLogResult(config, runId, false);

    // six planned units and three parts, three Documents each
    assertTrue(result.getStatus());
    assertEquals(27, numSucceeded(result));
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5", "p1", "p2", "p3"));

    // Resuming reads the whole log again. The parts are in it only as named by the reports of the units that handed
    // them back, followed by their own completions, and that is enough to know that all of them are done.
    RunResult again = Runner.startOrResumeAndLogResult(config, runId, false);
    assertTrue(again.getStatus());
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5", "p1", "p2", "p3"));
    assertEquals(1, ScriptedPartitionedConnector.finalizes.get());
  }

  @Test
  public void testResumeExecutesPartsHandedBackBeforeTheCoordinatorDied() throws Exception {
    String runId = "run-" + pipeline;
    // every planned unit hands back a part, and no part can finish until the gate opens
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    List<String> planned = List.of("u0", "u1", "u2", "u3", "u4", "u5");
    List<String> parts = List.of("p0", "p1", "p2", "p3", "p4", "p5");
    for (int i = 0; i < 6; i++) {
      ScriptedPartitionedConnector.handBacks.put(planned.get(i), List.of(parts.get(i)));
      ScriptedPartitionedConnector.gatedUnits.add(parts.get(i));
    }

    SilenceableRunControl firstControl = new SilenceableRunControl(new KafkaRunControl(config));
    CrawlCoordinator first = new CrawlCoordinator(config, runId, firstControl, replay -> new KafkaCoordinatorMessenger(config, replay));
    Thread firstThread = new Thread(() -> {
      try {
        first.run(false, false);
      } catch (Exception e) {
        // expected: the thread is interrupted below
      }
    });
    firstThread.start();

    // Kill the first Coordinator once a part is being executed. Other parts have been dispatched and not started,
    // or been reported and not dispatched, or belong to planned units that have not been executed yet.
    waitFor("a part to be executed", () -> !ScriptedPartitionedConnector.reachedGate.isEmpty());
    String[] doneBeforeDeath = ScriptedPartitionedConnector.completed.toArray(new String[0]);
    firstControl.silent = true;
    firstThread.interrupt();
    firstThread.join(30_000);
    assertFalse(firstThread.isAlive());

    Thread.sleep(4000);
    ScriptedPartitionedConnector.gate.countDown();

    RunResult result = Runner.resumeAndLogResult(config, runId, false, false);

    // every unit and every part was executed, whichever state the first Coordinator left it in
    assertTrue(result.getStatus());
    assertTrue(ScriptedPartitionedConnector.completed.containsAll(planned));
    assertTrue(ScriptedPartitionedConnector.completed.containsAll(parts));
    assertTrue("succeeded: " + numSucceeded(result), numSucceeded(result) >= 36);
    // and what was done before it died was not done again
    assertTrue(executedOnce(doneBeforeDeath));
  }

  @Test
  public void testFileConnectorSharesOutALargeTree() throws Exception {
    // three levels of directories, three wide, with two files in each: 13 directories and 26 files
    Path root = Files.createTempDirectory("crawl-tree");
    int numFiles = 0;
    try {
      List<Path> level = List.of(root);
      for (int depth = 0; depth < 3; depth++) {
        List<Path> next = new ArrayList<>();
        for (Path directory : level) {
          Files.writeString(directory.resolve("one.txt"), "one");
          Files.writeString(directory.resolve("two.txt"), "two");
          numFiles += 2;
          for (int i = 0; depth < 2 && i < 3; i++) {
            next.add(Files.createDirectory(directory.resolve("d" + i)));
          }
        }
        level = next;
      }

      // The whole tree is planned as one unit, and a unit may list three directories.
      Config fileConfig = ConfigFactory.parseString("""
          connectors: [{
            name: "files", class: "com.kmwllc.lucille.connector.FileConnector", pipeline: "%s",
            paths: ["%s"], partitioning { depth: 0, maxDirectoriesPerUnit: 3 }
          }]
          """.formatted(pipeline, root.toUri())).withFallback(config);

      // Crawlers execute units of the connectors they were configured with
      crawlerPool.stop();
      crawlerPool.join(10_000);
      crawlerPool = new CrawlerPool(fileConfig, CrawlerMessengerFactory.getKafkaFactory(fileConfig));
      crawlerPool.start();

      RunResult result = Runner.run(fileConfig, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

      // every file was published once, though no unit walked more than three of the thirteen directories
      assertTrue(result.getStatus());
      assertEquals(26, numFiles);
      assertEquals(26, numSucceeded(result));

      // 13 directories at 3 a unit cannot be done in fewer than 5 units
      try (KafkaConsumer<String, String> consumer = KafkaUtils.createUngroupedConsumer(fileConfig, "test", 10)) {
        List<TopicPartition> partitions = consumer.partitionsFor("work_" + pipeline).stream()
            .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
        long numUnits = consumer.endOffsets(partitions).values().stream().mapToLong(Long::longValue).sum();
        assertTrue("units: " + numUnits, numUnits >= 5 && numUnits <= 13);
      }
    } finally {
      try (Stream<Path> paths = Files.walk(root)) {
        paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
      }
    }
  }

  @Test
  public void testUnitThatHangsIsReportedAndExecutedAgain() throws Exception {
    Config limited = ConfigFactory.parseString("crawl.maxUnitSecs: 2").withFallback(config);
    crawlerPool.stop();
    crawlerPool.join(10_000);
    crawlerPool = new CrawlerPool(limited, CrawlerMessengerFactory.getKafkaFactory(limited));
    AtomicInteger exits = new AtomicInteger();
    crawlerPool.setExitAction(exits::incrementAndGet);
    crawlerPool.start();

    ScriptedPartitionedConnector.hang = new CountDownLatch(1);
    ScriptedPartitionedConnector.hangOnce.add("u3");

    RunResult result = Runner.run(limited, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    assertTrue(result.getStatus());
    assertEquals(18, numSucceeded(result));
    // Once as the attempt that hung and once as the attempt that replaced it. The attempt that hung was acknowledged,
    // so it was not delivered to another Crawler to hang that one too. And the Crawler that hung gave up its
    // partitions of the work topic, or the units on them, which may include the replacement attempt, would have
    // waited for it for ever.
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u3"));
    assertTrue(executedOnce("u0", "u1", "u2", "u4", "u5"));
    assertEquals(0, exits.get());
    ScriptedPartitionedConnector.hang.countDown();
  }

  @Test
  public void testUnitIsDispatchedAfterTheWorkTopicHasBeenIdle() throws Exception {
    // the producer forgets a topic it has not sent to for this long, and must fetch its metadata again at the next send
    Config idle = ConfigFactory.parseString("""
        kafka.producer { "metadata.max.idle.ms": 1000, "max.block.ms": 5000 }
        """).withFallback(config);
    // u0 hands a part back, but not until the other units are done and the work topic has been idle for a while.
    // Four of them at least: the two Crawler threads share four partitions, and a unit dealt to the other partition
    // of the thread that holds u0 waits behind it.
    ScriptedPartitionedConnector.handBacks.put("u0", List.of("p1"));
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    ScriptedPartitionedConnector.gatedUnits.add("u0");
    new Thread(() -> {
      try {
        waitFor("the other units to be done", () -> ScriptedPartitionedConnector.completed.size() >= 4);
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        ScriptedPartitionedConnector.gate.countDown();
      }
    }).start();

    RunResult result = Runner.run(idle, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    // the dispatch of p1 is the first send to the work topic after the gap
    assertTrue(result.getStatus());
    assertEquals(21, numSucceeded(result));
    assertTrue(executedOnce("u0", "u1", "u2", "u3", "u4", "u5", "p1"));
  }

  @Test
  public void testUnitCostsAreReadFromAnEarlierRun() throws Exception {
    String earlier = "run-" + pipeline;
    ScriptedPartitionedConnector.handBacks.put("u0", List.of("p1"));
    assertTrue(Runner.run(config, Runner.RunType.DISTRIBUTED_CRAWL, earlier).getStatus());

    KafkaCoordinatorMessenger messenger = new KafkaCoordinatorMessenger(config, false);
    try {
      messenger.initialize("run-costs-" + pipeline, pipeline);
      Map<String, Long> costs = messenger.readUnitCosts(earlier, pipeline);
      // every unit reported one source call
      assertEquals(7, costs.size());
      assertEquals(Long.valueOf(1), costs.get("connector1/u3"));
      assertEquals(Long.valueOf(1), costs.get("connector1/p1"));
      assertTrue(messenger.readUnitCosts("no-such-run", pipeline).isEmpty());
    } finally {
      messenger.close();
    }

    // and a run that is told to use them completes as any other
    ScriptedPartitionedConnector.reset();
    ScriptedPartitionedConnector.handBacks.put("u0", List.of("p1"));
    Config costed = ConfigFactory.parseString("crawl.costsFromRun: \"" + earlier + "\"").withFallback(config);
    RunResult result = Runner.run(costed, Runner.RunType.DISTRIBUTED_CRAWL, "run-costed-" + pipeline);
    assertTrue(result.getStatus());
    assertEquals(21, numSucceeded(result));
  }

  @Test
  public void testSourceAllowanceReachesCrawlersInTheHeartbeat() throws Exception {
    // 40 calls over 4 partitions: 10 a unit at most, starting from a quarter of that
    Config limited = ConfigFactory.parseString("crawl.maxSourceConcurrency: 40").withFallback(config);
    crawlerPool.stop();
    crawlerPool.join(10_000);
    crawlerPool = new CrawlerPool(limited, CrawlerMessengerFactory.getKafkaFactory(limited));
    crawlerPool.start();
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    ScriptedPartitionedConnector.gateAfter = 4;
    new Thread(() -> {
      try {
        Thread.sleep(2500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      ScriptedPartitionedConnector.gate.countDown();
    }).start();

    RunResult result = Runner.run(limited, Runner.RunType.DISTRIBUTED_CRAWL, "run-" + pipeline);

    assertTrue(result.getStatus());
    List<Integer> seen = ScriptedPartitionedConnector.allowancesSeen;
    assertEquals(6, seen.size());
    assertTrue(seen.toString(), seen.stream().allMatch(allowance -> allowance != null && allowance >= 2 && allowance <= 10));
    // it climbed while the first units were held: the later units saw more than the first
    assertTrue(seen.toString(), seen.get(seen.size() - 1) > seen.get(0));
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
    public Map<String, Status> list() throws Exception {
      return delegate.list();
    }

    @Override
    public void heartbeat(String runId, int epoch, String configHash, Integer unitConcurrency) throws Exception {
      if (!silent) {
        delegate.heartbeat(runId, epoch, configHash, unitConcurrency);
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
    CrawlCoordinator first = new CrawlCoordinator(config, runId, firstControl, replay -> new KafkaCoordinatorMessenger(config, replay));
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
        replay -> new KafkaCoordinatorMessenger(config, replay)).run(true, false);
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
