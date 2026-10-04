package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.ScriptedPartitionedConnector;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.message.LocalCrawlMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CountDownLatch;
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
    Config config = ConfigFactory.parseString(connectors + COMMON);
    LocalMessenger localMessenger = new LocalMessenger(config);
    messenger = new LocalCrawlMessenger(localMessenger, 60_000);

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
    return new CrawlCoordinator(config, runId, messenger, () -> messenger).run(false, false);
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
        WorkUnit.newPayload().put("unit", 0)));
    messenger.dispatchUnit(new WorkUnit("run1", "connector1", "other_pipeline", "connector1/forged1", 1, 1, "hash",
        WorkUnit.newPayload().put("unit", 0)));
    messenger.dispatchUnit(new WorkUnit("run1/../x", "connector1", "pipeline1", "connector1/forged2", 1, 1, "hash",
        WorkUnit.newPayload().put("unit", 0)));
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
