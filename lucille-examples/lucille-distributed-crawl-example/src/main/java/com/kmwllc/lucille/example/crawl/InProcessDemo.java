package com.kmwllc.lucille.example.crawl;

import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.CrawlCoordinator;
import com.kmwllc.lucille.core.CrawlerPool;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.WorkerPool;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.message.LocalCrawlMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Runs a whole distributed crawl inside one JVM, with in-memory queues standing in for Kafka: a Coordinator, a pool
 * of Crawler threads, Workers and an Indexer. Every role is the real one; only the transport differs. This is the
 * quickest way to watch the mechanics (units planned, executed, handed back) without a broker.
 *
 * <p>For a run across processes, start each role on its own against Kafka instead: see scripts/run-kafka.sh.
 *
 * <pre>
 *   java -Dconfig.file=conf/in-process.conf -cp 'target/lib/*:target/classes' \
 *       com.kmwllc.lucille.example.crawl.InProcessDemo
 * </pre>
 */
public class InProcessDemo {

  public static void main(String[] args) throws Exception {
    RunResult result = run(ConfigFactory.load(), "demo-" + UUID.randomUUID().toString().substring(0, 8));
    System.out.println(result);
    System.exit(result.getStatus() ? 0 : 1);
  }

  /**
   * Runs the crawl the config describes and returns its result. The config must name one pipeline, which the
   * Workers and Indexer serve.
   */
  public static RunResult run(Config config, String runId) throws Exception {
    String pipeline = config.getConfigList("pipelines").get(0).getString("name");

    // In memory, one queue plays every topic. The crawl messenger adds the work queue and the run's control records,
    // and its partition count is what the Coordinator paces dispatch by: one unit in flight per partition.
    CrawlConfig crawlConfig = new CrawlConfig(config);
    LocalMessenger messenger = new LocalMessenger(config);
    LocalCrawlMessenger crawlMessenger = new LocalCrawlMessenger(messenger,
        TimeUnit.SECONDS.toMillis(crawlConfig.orphanTimeoutSecs), crawlConfig.workTopicPartitions);

    // The long-lived roles start first, as they would in a deployment: they wait for work.
    WorkerPool workers = new WorkerPool(config, pipeline, runId, WorkerMessengerFactory.getConstantFactory(messenger), "demo");
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, false, "demo", runId);
    // as the Runner and the Indexer process do before indexing: checks the destination and, for CSV, writes the header
    if (!indexer.validateConnection()) {
      throw new IllegalStateException("The indexer's destination is not reachable.");
    }
    Thread indexerThread = new Thread(indexer, "demo-indexer");
    CrawlerPool crawlers = new CrawlerPool(config, CrawlerMessengerFactory.getConstantFactory(crawlMessenger));

    workers.start();
    indexerThread.start();
    crawlers.start();
    try {
      // The Coordinator: plans each Connector's units, dispatches them, and waits for every unit and Document.
      return new CrawlCoordinator(config, runId, crawlMessenger, replay -> crawlMessenger).run(false, false);
    } finally {
      crawlers.stop();
      crawlers.join(10_000);
      workers.stop();
      workers.join(10_000);
      indexer.terminate();
      indexerThread.join(10_000);
    }
  }
}
