package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.ScriptedPartitionedConnector;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.message.LocalCrawlMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.RunControl;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Runs distributed crawls with every component as a thread of this JVM, communicating in memory. Covers what the
 * Coordinator and Crawlers do with each other; the Kafka-specific behavior is covered by KafkaDistributedCrawlTest.
 */
public class DistributedCrawlTest {

  private static final String COMMON = """
      pipelines: [{name: "pipeline1", stages: [{class: "com.kmwllc.lucille.stage.NopStage"}]}]
      solr { url: ["http://localhost:null"] }
      indexer { sendEnabled: false, type: "solr" }
      crawl { threads: 3, maxAttempts: 2, heartbeatSecs: 1, orphanTimeoutSecs: 60 }
      """;

  private static final String SCRIPTED = """
      connectors: [{
        name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
        pipeline: "pipeline1", numUnits: 6, docsPerUnit: 5
      }]
      """;

  private LocalCrawlMessenger messenger;
  private WorkerPool workerPool;
  private Indexer indexer;
  private Thread indexerThread;
  private CrawlerPool crawlerPool;
  // While this is closed, the Coordinator's read of the run's Events does not return.
  private volatile CountDownLatch eventsBlocked = new CountDownLatch(0);

  @Before
  public void setUp() {
    ScriptedPartitionedConnector.reset();
  }

  @After
  public void tearDown() throws Exception {
    crawlerPool.stop();
    crawlerPool.join(5000);
    workerPool.stop();
    workerPool.join(5000);
    indexer.terminate();
    indexerThread.join(5000);
  }

  // Starts Crawlers, Workers and an Indexer, as a deployment would have running before any run begins.
  private Config start(String connectors) throws Exception {
    return start(connectors, "");
  }

  private Config start(String connectors, String settings) throws Exception {
    Config config = ConfigFactory.parseString(connectors + COMMON + settings);
    LocalMessenger localMessenger = new LocalMessenger(config);
    messenger = new LocalCrawlMessenger(localMessenger, 60_000, new CrawlConfig(config).workTopicPartitions) {
      @Override
      public Event pollEvent() throws Exception {
        eventsBlocked.await();
        return super.pollEvent();
      }
    };

    workerPool = new WorkerPool(config, "pipeline1", null, WorkerMessengerFactory.getConstantFactory(localMessenger), "test");
    workerPool.start();
    indexer = IndexerFactory.fromConfig(config, localMessenger, true, "test");
    indexerThread = new Thread(indexer);
    indexerThread.start();
    crawlerPool = new CrawlerPool(config, CrawlerMessengerFactory.getConstantFactory(messenger));
    crawlerPool.start();
    return config;
  }

  private RunResult run(Config config, String runId) throws Exception {
    return new CrawlCoordinator(config, runId, messenger, replay -> messenger).run(false, false);
  }

  static long numSucceeded(RunResult result) {
    Matcher matcher = Pattern.compile("(\\d+) docs succeeded").matcher(result.toString());
    long total = 0;
    while (matcher.find()) {
      total += Long.parseLong(matcher.group(1));
    }
    return total;
  }

  @Test
  public void testEveryUnitIsExecutedOnce() throws Exception {
    RunResult result = run(start(SCRIPTED), "run1");

    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
    for (int i = 0; i < 6; i++) {
      assertEquals(1, ScriptedPartitionedConnector.executionsOf("u" + i));
    }

    // each lifecycle method runs once, on the Coordinator, however many Crawlers execute units
    assertEquals(1, ScriptedPartitionedConnector.preExecutes.get());
    assertEquals(1, ScriptedPartitionedConnector.prepares.get());
    assertEquals(1, ScriptedPartitionedConnector.plans.get());
    assertEquals(1, ScriptedPartitionedConnector.finalizes.get());
    assertEquals(1, ScriptedPartitionedConnector.postExecutes.get());
  }

  @Test
  public void testHandedBackPartsAreExecutedAsUnits() throws Exception {
    Config config = start(SCRIPTED);
    // u0 hands back two parts, and one of those hands back a third
    ScriptedPartitionedConnector.handBacks.put("u0", List.of("p1", "p2"));
    ScriptedPartitionedConnector.handBacks.put("p1", List.of("p3"));
    // u4 hands back a part that another unit hands back as well, and a unit the planner made
    ScriptedPartitionedConnector.handBacks.put("u4", List.of("p2", "u5"));

    RunResult result = run(config, "run1");

    // six planned units and three parts, five Documents each
    assertTrue(result.getStatus());
    assertEquals(45, numSucceeded(result));
    for (String unitKey : List.of("u0", "u1", "u2", "u3", "u4", "u5", "p1", "p2", "p3")) {
      assertEquals(unitKey, 1, ScriptedPartitionedConnector.executionsOf(unitKey));
    }
    assertEquals(1, ScriptedPartitionedConnector.finalizes.get());
  }

  @Test
  public void testHandedBackPartsOfAFailedExecutionAreNotExecutedUntilItSucceeds() throws Exception {
    Config config = start(SCRIPTED);
    ScriptedPartitionedConnector.handBacks.put("u2", List.of("p1"));
    ScriptedPartitionedConnector.failOnce.add("u2");
    ScriptedPartitionedConnector.failOnce.add("p1");

    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(35, numSucceeded(result));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u2"));
    // a part is retried like any unit
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("p1"));
  }

  @Test
  public void testUnitThatHangsIsReportedAndExecutedAgain() throws Exception {
    Config config = start(SCRIPTED, "crawl.maxUnitSecs: 1");
    ScriptedPartitionedConnector.hang = new CountDownLatch(1);
    ScriptedPartitionedConnector.hangOnce.add("u2");

    // the Crawler executing u2 never returns from it, and without the limit the run would never end
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u2"));

    // The Crawler started in place of the one that hung keeps the process alive as the others do. A thread takes
    // after the thread that starts it, and this one is started by the watchdog, which is a daemon.
    List<Thread> crawlerThreads = Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.isAlive() && thread.getName().contains("Crawler-")).toList();
    assertTrue(crawlerThreads.size() >= 3);
    assertTrue(crawlerThreads.toString(), crawlerThreads.stream().noneMatch(Thread::isDaemon));

    // a connector that looks can see that its unit has been given up on, and stop
    ScriptedPartitionedConnector.hang.countDown();
    long deadline = System.currentTimeMillis() + 10_000;
    while (!ScriptedPartitionedConnector.sawCancelled.contains("u2") && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(java.util.Set.of("u2"), ScriptedPartitionedConnector.sawCancelled);
  }

  @Test
  public void testUnitThatAlwaysHangsFailsTheRun() throws Exception {
    Config config = start(SCRIPTED, "crawl.maxUnitSecs: 1");
    ScriptedPartitionedConnector.hang = new CountDownLatch(1);
    // crawl.maxAttempts is 2
    ScriptedPartitionedConnector.hangAlways.add("u2");

    RunResult result = run(config, "run1");

    // a hang counts against the unit's attempts, so a unit that hangs every time ends the run instead of stalling it
    assertFalse(result.getStatus());
    assertTrue(result.toString(), result.toString().contains("Timed out"));
  }

  @Test
  public void testCrawlerExitsAfterAUnitTimesOutIfConfiguredTo() throws Exception {
    Config config = start(SCRIPTED, "crawl { maxUnitSecs: 1, exitOnTimeout: true }");
    AtomicInteger exits = new AtomicInteger();
    CrawlerPool first = crawlerPool;
    // in place of the process exiting and being started again by whatever runs it
    first.setExitAction(() -> {
      exits.incrementAndGet();
      try {
        crawlerPool = new CrawlerPool(config, CrawlerMessengerFactory.getConstantFactory(messenger));
        crawlerPool.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    ScriptedPartitionedConnector.hang = new CountDownLatch(1);
    ScriptedPartitionedConnector.hangOnce.add("u2");

    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u2"));
    // once for the one unit that timed out, not once each time the watchdog looks
    assertEquals(1, exits.get());
    // before "exiting", the pool's other Crawlers were stopped, so that they give their work up at once
    first.join(5000);
  }

  @Test
  public void testCrawlerDoesNotExitByDefault() throws Exception {
    Config config = start(SCRIPTED, "crawl.maxUnitSecs: 1");
    AtomicInteger exits = new AtomicInteger();
    crawlerPool.setExitAction(exits::incrementAndGet);
    ScriptedPartitionedConnector.hang = new CountDownLatch(1);
    ScriptedPartitionedConnector.hangOnce.add("u2");

    assertTrue(run(config, "run1").getStatus());
    assertEquals(0, exits.get());
  }

  @Test
  public void testStuckCoordinatorStopsItsHeartbeatAndExitsWithoutCancellingTheRun() throws Exception {
    Config config = ConfigFactory.parseString("crawl.orphanTimeoutSecs: 2").withFallback(start(SCRIPTED));
    CrawlCoordinator coordinator = new CrawlCoordinator(config, "run1", messenger, replay -> messenger);
    CountDownLatch stuck = new CountDownLatch(1);
    coordinator.setStuckAction(stuck::countDown);
    AtomicReference<RunResult> result = new AtomicReference<>();

    // the Coordinator's read of the Event log blocks, as a call to a broker that never answers would
    eventsBlocked = new CountDownLatch(1);
    Thread runThread = new Thread(() -> {
      try {
        result.set(coordinator.run(false, false));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    runThread.start();

    // its heartbeat comes from another thread, and would otherwise go on saying the run is in hand
    assertTrue(stuck.await(20, java.util.concurrent.TimeUnit.SECONDS));
    Thread.sleep(2500);
    RunControl.Status status = messenger.latest("run1");
    assertTrue("age " + status.ageMillis(), status.ageMillis() >= 2000);
    // The run is not cancelled: it is left as a run whose Coordinator died, which is one that can be resumed.
    assertFalse(status.cancelled());

    eventsBlocked.countDown();
    runThread.join(30_000);
    assertFalse(runThread.isAlive());
    assertFalse(messenger.latest("run1").cancelled());
  }

  @Test
  public void testCoordinatorThatIsNotStuckIsLeftAlone() throws Exception {
    // units that take longer than the orphan timeout do not make the Coordinator look stuck: it is still reading Events
    Config config = ConfigFactory.parseString("crawl.orphanTimeoutSecs: 2").withFallback(start(SCRIPTED));
    CrawlCoordinator coordinator = new CrawlCoordinator(config, "run1", messenger, replay -> messenger);
    AtomicBoolean stuck = new AtomicBoolean();
    coordinator.setStuckAction(() -> stuck.set(true));
    ScriptedPartitionedConnector.gateAfter = 0;
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);

    Thread opener = new Thread(() -> {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      ScriptedPartitionedConnector.gate.countDown();
    });
    opener.start();

    assertTrue(coordinator.run(false, false).getStatus());
    assertFalse(stuck.get());
    opener.join();
  }

  @Test
  public void testUnitsAreToldTheirShareOfTheSourceConcurrency() throws Exception {
    // 30 calls for the run over 3 partitions is 10 a unit; starting at the maximum, so that is what every unit sees
    Config config = start(SCRIPTED, "crawl { maxSourceConcurrency: 30, initialSourceConcurrency: 30, workTopicPartitions: 3 }");

    assertTrue(run(config, "run1").getStatus());

    assertEquals(6, ScriptedPartitionedConnector.allowancesSeen.size());
    assertTrue(ScriptedPartitionedConnector.allowancesSeen.toString(),
        ScriptedPartitionedConnector.allowancesSeen.stream().allMatch(allowance -> allowance != null && allowance == 10));
  }

  @Test
  public void testWithoutALimitUnitsAreToldNothing() throws Exception {
    assertTrue(run(start(SCRIPTED), "run1").getStatus());
    assertTrue(ScriptedPartitionedConnector.allowancesSeen.stream().allMatch(allowance -> allowance == null));
  }

  @Test
  public void testSourceConcurrencyIsCutWhenTheSourceRefusesCalls() throws Exception {
    // 3 units in flight at 10 calls each is 30 against a source that takes 12; after a heartbeat with refusals the
    // run's figure is halved and the units see 5
    Config config = start("""
        connectors: [{
          name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
          pipeline: "pipeline1", numUnits: 40, docsPerUnit: 1
        }]
        """, "crawl { maxSourceConcurrency: 30, initialSourceConcurrency: 30, sourceConcurrencyStep: 3, "
        + "sourceConcurrencyHoldSecs: 2, workTopicPartitions: 3 }");
    ScriptedPartitionedConnector.sourceLimit = 12;
    ScriptedPartitionedConnector.sourceHoldMillis = 300;
    CrawlCoordinator coordinator = new CrawlCoordinator(config, "run1", messenger, replay -> messenger);

    RunResult result = coordinator.run(false, false);

    assertTrue(result.getStatus());
    assertTrue("the source refused nothing", ScriptedPartitionedConnector.sourceRefused.get() > 0);
    List<Integer> seen = ScriptedPartitionedConnector.allowancesSeen;
    assertTrue(seen.toString(), seen.contains(10));
    assertTrue("the allowance never came down: " + seen, seen.stream().anyMatch(allowance -> allowance != null && allowance <= 5));
    // and the run's figure was cut: it did not just climb to the maximum and stay there
    assertTrue(String.valueOf(coordinator.currentSourceConcurrency()), coordinator.currentSourceConcurrency() < 30
        || seen.stream().anyMatch(allowance -> allowance != null && allowance < 10));
  }

  @Test
  public void testSourceConcurrencyBelowThePartitionsRunsFewerUnitsAtOnce() throws Exception {
    // three Crawler threads and partitions, but the source may see one call at a time: one unit at a time
    Config config = start(SCRIPTED, "crawl { maxSourceConcurrency: 1, workTopicPartitions: 3 }");
    ScriptedPartitionedConnector.gateAfter = 0;
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    new Thread(() -> {
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      ScriptedPartitionedConnector.gate.countDown();
    }).start();

    assertTrue(run(config, "run1").getStatus());
    // the first unit was held for 1.5 s and nothing else started meanwhile; every unit saw the one-call allowance
    assertTrue(ScriptedPartitionedConnector.allowancesSeen.stream().allMatch(allowance -> allowance != null && allowance == 1));
    assertEquals(1, ScriptedPartitionedConnector.maxConcurrentExecutions.get());
  }

  @Test
  public void testUnitThrottledPartWayHandsTheRestBackAndCompletes() throws Exception {
    Config config = start(SCRIPTED);
    ScriptedPartitionedConnector.throttledOnce.add("u1");

    RunResult result = run(config, "run1");

    // the half that was published before the throttle, plus the handed-back rest as a unit of its own
    assertTrue(result.getStatus());
    assertEquals(1, ScriptedPartitionedConnector.executionsOf("u1"));
    assertEquals(1, ScriptedPartitionedConnector.executionsOf("u1-rest"));
    assertEquals(32, numSucceeded(result));
  }

  @Test
  public void testUnitThatFailsThrottledIsExecutedAgainAfterAWait() throws Exception {
    // crawl.maxAttempts is 2: two throttled failures would end the run if they counted
    Config config = start(SCRIPTED, "crawl { throttleBackoffSecs: 1, throttleBackoffCapSecs: 1 }");
    ScriptedPartitionedConnector.throttleFailOnce.add("u3");
    ScriptedPartitionedConnector.throttleFailTwice.add("u4");

    long start = System.currentTimeMillis();
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u3"));
    assertEquals(3, ScriptedPartitionedConnector.executionsOf("u4"));
    // u4 waited out two backoffs of at least half a second each
    assertTrue(System.currentTimeMillis() - start >= 1000);
  }

  @Test
  public void testUnitRefusedTooOftenFailsTheRun() throws Exception {
    Config config = start(SCRIPTED, "crawl { maxThrottledAttempts: 2, throttleBackoffSecs: 1, throttleBackoffCapSecs: 1 }");
    ScriptedPartitionedConnector.throttleFailAlways.add("u2");

    RunResult result = run(config, "run1");

    assertFalse(result.getStatus());
    assertTrue(result.toString(), result.toString().contains("refused"));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u2"));
  }

  @Test
  public void testFailedUnitIsExecutedAgain() throws Exception {
    Config config = start(SCRIPTED);
    ScriptedPartitionedConnector.failOnce.add("u2");
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u2"));
    assertEquals(1, ScriptedPartitionedConnector.executionsOf("u3"));
  }

  @Test
  public void testUnitThatKeepsFailingFailsTheRun() throws Exception {
    Config config = start(SCRIPTED);
    ScriptedPartitionedConnector.failAlways.add("u1");
    RunResult result = run(config, "run1");

    assertFalse(result.getStatus());
    assertTrue(result.toString().contains("connector1/u1 failed after 2 attempts"));
    assertTrue(result.toString().contains("Scripted failure of u1"));
    assertEquals(2, ScriptedPartitionedConnector.executionsOf("u1"));
    assertEquals(0, ScriptedPartitionedConnector.finalizes.get());
    assertEquals(0, ScriptedPartitionedConnector.postExecutes.get());
  }

  @Test
  public void testDocumentsPublishedWhenFinalizingAreWaitedFor() throws Exception {
    Config config = start(SCRIPTED);
    ScriptedPartitionedConnector.finalizeDocId = "tombstone";
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(31, numSucceeded(result));
  }

  @Test
  public void testConnectorWithoutPartitioningRunsAsOneUnit() throws Exception {
    Config config = start("""
        connectors: [{name: "connector1", class: "com.kmwllc.lucille.connector.SequenceConnector", pipeline: "pipeline1", numDocs: 7}]
        """);
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(7, numSucceeded(result));
  }

  @Test
  public void testConnectorsRunInSequence() throws Exception {
    Config config = start("""
        connectors: [
          {name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
           pipeline: "pipeline1", numUnits: 2, docsPerUnit: 5},
          {name: "connector2", class: "com.kmwllc.lucille.connector.SequenceConnector", pipeline: "pipeline1",
           numDocs: 10, docIdPrefix: "seq-", partitioning { unitSize: 3 }}
        ]
        """);
    RunResult result = run(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(20, numSucceeded(result));
    assertTrue(result.toString().contains("connector1: complete. 10 docs succeeded."));
    assertTrue(result.toString().contains("connector2: complete. 10 docs succeeded."));
  }

  @Test
  public void testFailedConnectorStopsTheRun() throws Exception {
    Config config = start("""
        connectors: [
          {name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
           pipeline: "pipeline1", numUnits: 2, docsPerUnit: 5},
          {name: "connector2", class: "com.kmwllc.lucille.connector.SequenceConnector", pipeline: "pipeline1", numDocs: 10}
        ]
        """);
    ScriptedPartitionedConnector.failAlways.add("u0");
    RunResult result = run(config, "run1");

    assertFalse(result.getStatus());
    assertFalse(result.toString().contains("connector2: complete"));
  }

  @Test
  public void testRunIdCannotBeReused() throws Exception {
    Config config = start(SCRIPTED);
    assertTrue(run(config, "run1").getStatus());

    ScriptedPartitionedConnector.reset();
    assertFalse(run(config, "run1").getStatus());
    assertEquals(0, ScriptedPartitionedConnector.plans.get());
  }

  @Test
  public void testRunIdIsNotSpentIfTheRunCannotStart() throws Exception {
    Config config = start(SCRIPTED);

    // the Coordinator cannot attach to the run's event topic, as when Kafka does not yet show a topic just created
    RunResult failed = new CrawlCoordinator(config, "run1", messenger, replay -> {
      throw new IllegalStateException("Event topic pipeline1_event_run1 had no partitions visible");
    }).run(false, false);

    assertFalse(failed.getStatus());
    assertTrue(failed.toString().contains("had no partitions visible"));
    // nothing was announced or begun for the run
    assertNull(messenger.latest("run1"));
    assertEquals(0, ScriptedPartitionedConnector.preExecutes.get());

    // so the same run ID can simply be used again
    RunResult result = run(config, "run1");
    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
  }

  private RunResult startOrResume(Config config, String runId) throws Exception {
    return new CrawlCoordinator(config, runId, messenger, replay -> messenger).startOrResume();
  }

  @Test
  public void testStartOrResumeStartsARunThatIsNew() throws Exception {
    Config config = start(SCRIPTED);
    RunResult result = startOrResume(config, "run1");

    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
    assertEquals(1, messenger.latest("run1").epoch());
    assertEquals("complete", messenger.latest("run1").reason());
  }

  @Test
  public void testStartOrResumeResumesARunWhoseCoordinatorHasGoneSilent() throws Exception {
    // the Coordinator being started treats two seconds without a heartbeat as silence
    Config config = ConfigFactory.parseString("crawl.orphanTimeoutSecs: 2").withFallback(start(SCRIPTED));

    // an earlier Coordinator announced the run a moment ago and then died
    messenger.heartbeat("run1", 1, CrawlConfig.runConfigHash(config));

    long start = System.currentTimeMillis();
    RunResult result = startOrResume(config, "run1");
    long waited = System.currentTimeMillis() - start;

    // it did not take the run over while the heartbeat was fresh, and did once it was not
    assertTrue("waited " + waited + " ms", waited >= 1500);
    assertTrue(result.getStatus());
    assertEquals(2, messenger.latest("run1").epoch());
  }

  @Test
  public void testStartOrResumeGivesUpIfAnotherCoordinatorIsAlive() throws Exception {
    Config config = ConfigFactory.parseString("crawl.orphanTimeoutSecs: 2").withFallback(start(SCRIPTED));
    String configHash = CrawlConfig.runConfigHash(config);

    // another Coordinator has the run and keeps saying so
    AtomicBoolean alive = new AtomicBoolean(true);
    Thread other = new Thread(() -> {
      while (alive.get()) {
        messenger.heartbeat("run1", 1, configHash);
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    other.start();

    try {
      RunResult result = startOrResume(config, "run1");

      assertFalse(result.getStatus());
      // the run is left exactly as it was
      assertEquals(1, messenger.latest("run1").epoch());
      assertFalse(messenger.latest("run1").cancelled());
      assertEquals(0, ScriptedPartitionedConnector.preExecutes.get());
    } finally {
      alive.set(false);
      other.join();
    }
  }

  @Test
  public void testStartOrResumeOfACompletedRunDoesNotRunItAgain() throws Exception {
    Config config = start(SCRIPTED);
    assertTrue(startOrResume(config, "run1").getStatus());
    assertEquals(1, ScriptedPartitionedConnector.plans.get());

    // A real Coordinator would learn from the run's Events that every connector had completed. This messenger
    // keeps no Events, so the second Coordinator does the work again; what is checked here is that a run that has
    // ended is resumed at once, without waiting, and under a new epoch.
    long start = System.currentTimeMillis();
    assertTrue(startOrResume(config, "run1").getStatus());
    assertTrue(System.currentTimeMillis() - start < 30_000);
    assertEquals(2, messenger.latest("run1").epoch());
  }

  @Test
  public void testStartOrResumeRefusesAChangedConfig() throws Exception {
    Config config = start(SCRIPTED);
    assertTrue(startOrResume(config, "run1").getStatus());

    Config changed = ConfigFactory.parseString(SCRIPTED.replace("numUnits: 6", "numUnits: 7") + COMMON);
    assertFalse(startOrResume(changed, "run1").getStatus());
    assertEquals(1, messenger.latest("run1").epoch());
  }

  @Test
  public void testListRuns() throws Exception {
    Config config = start(SCRIPTED);
    assertTrue(run(config, "run1").getStatus());
    ScriptedPartitionedConnector.failAlways.add("u0");
    assertFalse(run(config, "run2").getStatus());
    messenger.heartbeat("run3", 4, "hash");

    Map<String, RunControl.Status> runs = messenger.list();
    assertEquals(List.of("run1", "run2", "run3"), List.copyOf(runs.keySet()));
    assertEquals("complete", runs.get("run1").reason());
    assertEquals("failed", runs.get("run2").reason());
    assertFalse(runs.get("run3").cancelled());
    assertEquals(4, runs.get("run3").epoch());

    String table = Runner.formatRuns(runs, 120);
    assertTrue(table, table.contains("ended (complete)"));
    assertTrue(table, table.contains("ended (failed)"));
    assertTrue(table, table.contains("running"));
    // a run whose Coordinator has not been heard from for longer than the orphan timeout can be resumed
    assertTrue(Runner.formatRuns(Map.of("old", new RunControl.Status(false, 1, "hash", 300_000, null)), 120)
        .contains("silent, resumable"));
    assertEquals("No distributed crawls are on record.", Runner.formatRuns(Map.of(), 120));
  }

  @Test
  public void testRunIdMustBeUsableInATopicName() throws Exception {
    Config config = start(SCRIPTED);

    assertFalse(run(config, "run 1/../x").getStatus());
    assertFalse(run(config, "").getStatus());
    assertEquals(0, ScriptedPartitionedConnector.plans.get());
  }

  @Test
  public void testCrawlerDiscardsUnitItCannotRoute() throws Exception {
    Config config = start(SCRIPTED);

    // Units that did not come from a Coordinator: one naming a pipeline the Crawlers do not have, one whose run ID
    // could not be part of a topic name, and one for a run that no Coordinator has ever announced.
    messenger.dispatchUnit(new WorkUnit("ghost-run", "connector1", "pipeline1", "connector1/forged3", 1, 1, "hash",
        WorkUnit.newPayload().put("unit", 0)), 0);
    messenger.dispatchUnit(new WorkUnit("run1", "connector1", "other_pipeline", "connector1/forged1", 1, 1, "hash",
        WorkUnit.newPayload().put("unit", 0)), 0);
    messenger.dispatchUnit(new WorkUnit("run1/../x", "connector1", "pipeline1", "connector1/forged2", 1, 1, "hash",
        WorkUnit.newPayload().put("unit", 0)), 0);
    RunResult result = run(config, "run1");

    // they are discarded without being executed, and the Crawlers carry on with the real units
    assertTrue(result.getStatus());
    assertEquals(30, numSucceeded(result));
    assertEquals(6, ScriptedPartitionedConnector.executions.size());
    assertEquals(1, ScriptedPartitionedConnector.executionsOf("u0"));
  }

  @Test
  public void testRunStopsWhenTakenOverByAnotherCoordinator() throws Exception {
    Config config = start(SCRIPTED);
    // no unit can finish, so the run would otherwise wait for its timeout
    ScriptedPartitionedConnector.gate = new CountDownLatch(1);
    ScriptedPartitionedConnector.gateAfter = 0;

    AtomicReference<RunResult> result = new AtomicReference<>();
    Thread coordinator = new Thread(() -> {
      try {
        result.set(run(config, "run1"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    coordinator.start();

    // a Coordinator that resumed the run announces itself under the next epoch
    while (messenger.latest("run1") == null) {
      Thread.sleep(50);
    }
    messenger.heartbeat("run1", 2, "hash");

    coordinator.join(20_000);
    ScriptedPartitionedConnector.gate.countDown();
    assertFalse(coordinator.isAlive());
    assertFalse(result.get().getStatus());
    assertTrue(result.get().toString().contains("taken over by another Coordinator"));
    // it leaves the run to its new owner rather than announcing the end of it
    assertFalse(messenger.latest("run1").cancelled());
    assertEquals(0, ScriptedPartitionedConnector.postExecutes.get());
  }

  @Test
  public void testCrawlerRefusesUnitPlannedFromAnotherConfig() throws Exception {
    Config crawlerConfig = start(SCRIPTED);
    // the Coordinator was given a config in which the connector produces more per unit than the Crawlers expect
    Config coordinatorConfig = ConfigFactory.parseString(SCRIPTED.replace("docsPerUnit: 5", "docsPerUnit: 50") + COMMON);
    assertEquals(crawlerConfig.getConfig("crawl"), coordinatorConfig.getConfig("crawl"));

    RunResult result = run(coordinatorConfig, "run1");

    assertFalse(result.getStatus());
    assertTrue(result.toString().contains("differs between this Crawler and the run's Coordinator"));
    assertTrue(ScriptedPartitionedConnector.executions.isEmpty());
  }
}
