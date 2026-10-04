package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs <code>crawl.threads</code> Crawlers as threads, each with its own messenger, so that one process can execute
 * that many work units at once.
 */
public class CrawlerPool {

  private static final Logger log = LoggerFactory.getLogger(CrawlerPool.class);

  private final Config config;
  private final CrawlerMessengerFactory messengerFactory;
  private final int numCrawlers;
  private final List<Crawler> crawlers = new ArrayList<>();
  private final List<Thread> threads = new ArrayList<>();
  private boolean started = false;

  public CrawlerPool(Config config, CrawlerMessengerFactory messengerFactory) {
    this.config = config;
    this.messengerFactory = messengerFactory;
    this.numCrawlers = new CrawlConfig(config).threads;
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("CrawlerPool can be started at most once");
    }
    started = true;
    log.info("Starting {} crawler threads", numCrawlers);

    // distinguishes this pool's crawlers from those of other processes in what they report to the Coordinator
    String poolId = UUID.randomUUID().toString().substring(0, 8);

    try {
      for (int i = 0; i < numCrawlers; i++) {
        String name = "Crawler-" + poolId + "-" + (i + 1);
        Crawler crawler = new Crawler(config, messengerFactory.create(), name);
        crawlers.add(crawler);

        Thread thread = new Thread(crawler, ThreadNameUtils.createName(name));
        threads.add(thread);
        thread.start();
      }
    } catch (Exception e) {
      log.error("Exception caught when starting Crawler threads; aborting");
      stop();
      throw e;
    }
  }

  public void stop() {
    log.debug("Stopping {} crawler threads", threads.size());
    for (Crawler crawler : crawlers) {
      crawler.terminate();
    }
    messengerFactory.close();
  }

  public void join() throws InterruptedException {
    for (Thread thread : threads) {
      thread.join();
    }
  }

  public void join(long millis) throws InterruptedException {
    for (Thread thread : threads) {
      thread.join(millis);
    }
  }
}
