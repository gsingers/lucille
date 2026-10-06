package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs <code>crawl.threads</code> Crawlers as threads, each with its own messenger, so that one process can execute
 * that many work units at once.
 */
public class CrawlerPool {

  private static final Logger log = LoggerFactory.getLogger(CrawlerPool.class);
  private static final long WATCHDOG_PERIOD_MILLIS = 500;
  private static final long EXIT_GRACE_MILLIS = 10_000;
  private static final long JOIN_POLL_MILLIS = 500;

  private final Config config;
  private final CrawlerMessengerFactory messengerFactory;
  private final int numCrawlers;
  // replaced by the watchdog thread while other threads read them
  private final List<Crawler> crawlers = new CopyOnWriteArrayList<>();
  private final List<Thread> threads = new CopyOnWriteArrayList<>();
  private boolean started = false;
  private volatile boolean stopped = false;
  // distinguishes this pool's crawlers from those of other processes in what they report to the Coordinator
  private final String poolId = UUID.randomUUID().toString().substring(0, 8);
  private final AtomicInteger crawlersCreated = new AtomicInteger();

  private final Integer maxUnitSecs;
  private final int heartbeatSecs;
  private final boolean exitOnTimeout;
  private ScheduledExecutorService watchdog;
  // what "exit" means; replaced in tests, which cannot have the JVM exit
  private Runnable exitAction = () -> System.exit(1);

  public CrawlerPool(Config config, CrawlerMessengerFactory messengerFactory) {
    this.config = config;
    this.messengerFactory = messengerFactory;
    CrawlConfig crawlConfig = new CrawlConfig(config);
    this.numCrawlers = crawlConfig.threads;
    this.maxUnitSecs = crawlConfig.maxUnitSecs;
    this.heartbeatSecs = crawlConfig.heartbeatSecs;
    this.exitOnTimeout = crawlConfig.exitOnTimeout;
  }

  // package access for unit tests
  void setExitAction(Runnable exitAction) {
    this.exitAction = exitAction;
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("CrawlerPool can be started at most once");
    }
    started = true;
    log.info("Starting {} crawler threads", numCrawlers);

    try {
      for (int i = 0; i < numCrawlers; i++) {
        startCrawler(i);
      }
    } catch (Exception e) {
      log.error("Exception caught when starting Crawler threads; aborting");
      stop();
      throw e;
    }

    startTimer();
  }

  // Starts a Crawler in the given position, in place of the one that was there if there was one.
  private void startCrawler(int position) throws Exception {
    String name = "Crawler-" + poolId + "-" + crawlersCreated.incrementAndGet();
    Crawler crawler = new Crawler(config, messengerFactory.create(), name);
    Thread thread = new Thread(crawler, ThreadNameUtils.createName(name));
    // A thread takes after the one that starts it, and a replacement is started by the watchdog, which is a daemon.
    // Crawlers are what keep the process alive.
    thread.setDaemon(false);

    if (position < crawlers.size()) {
      crawlers.set(position, crawler);
      threads.set(position, thread);
    } else {
      crawlers.add(crawler);
      threads.add(thread);
    }
    thread.start();
  }

  /**
   * Watches for a unit that has been executing for longer than crawl.maxUnitSecs, as WorkerPool watches for a Worker
   * that has stopped polling. The unit's Crawler reports it as failed, so that it is dispatched again, and gives up
   * its place among the Crawlers. Its thread may be blocked for good and cannot be stopped, so it is left behind,
   * holding whatever it holds, and a new Crawler is started in its place. If crawl.exitOnTimeout is set the process
   * exits instead, for whatever started it to start a clean one.
   */
  private void startTimer() {
    BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("CrawlerWatchdog")).daemon(true).build();
    watchdog = Executors.newSingleThreadScheduledExecutor(threadFactory);

    // progress reports, every heartbeat, so that the Coordinator hears of refused calls before a unit ends
    watchdog.scheduleWithFixedDelay(() -> {
      for (Crawler crawler : crawlers) {
        if (!stopped) {
          crawler.reportProgress();
        }
      }
    }, heartbeatSecs, heartbeatSecs, TimeUnit.SECONDS);

    if (maxUnitSecs == null) {
      return;
    }

    watchdog.scheduleWithFixedDelay(() -> {
      for (int i = 0; i < crawlers.size(); i++) {
        if (stopped || !crawlers.get(i).reportIfTimedOut(TimeUnit.SECONDS.toMillis(maxUnitSecs))) {
          continue;
        }

        if (exitOnTimeout) {
          log.error("Shutting down because a unit exceeded the maximum allowed time of {} secs.", maxUnitSecs);
          // the Crawler that is stuck is not one of those to wait for
          crawlers.remove(i);
          threads.remove(i);
          // On a thread that keeps the process alive, unlike this one. Otherwise the process can end by itself,
          // reporting success, when the last Crawler stops, before it has been made to exit with an error.
          Thread exitThread = new Thread(this::exit, ThreadNameUtils.createName("CrawlerExit"));
          exitThread.setDaemon(false);
          exitThread.start();
          return;
        }

        try {
          startCrawler(i);
        } catch (Exception e) {
          log.error("Could not replace a Crawler whose unit timed out; the pool has one fewer.", e);
        }
      }
    }, WATCHDOG_PERIOD_MILLIS, WATCHDOG_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
  }

  // The other Crawlers are given a little time to finish their units and leave the consumer group. One that is
  // cut off instead keeps its share of the work topic until its session times out, and the units there wait.
  private void exit() {
    stopped = true;
    crawlers.forEach(Crawler::terminate);
    try {
      join(EXIT_GRACE_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    exitAction.run();
  }

  public void stop() {
    log.debug("Stopping {} crawler threads", threads.size());
    stopped = true;
    if (watchdog != null) {
      watchdog.shutdownNow();
    }
    for (Crawler crawler : crawlers) {
      crawler.terminate();
    }
    messengerFactory.close();
  }

  /**
   * Waits for the pool's Crawlers to stop. A Crawler that was replaced because its unit timed out is not waited
   * for: it may never stop.
   */
  public void join() throws InterruptedException {
    join(Long.MAX_VALUE);
  }

  /** Waits as join() does, for at most the given time in all. */
  public void join(long millis) throws InterruptedException {
    long deadline = millis == Long.MAX_VALUE ? Long.MAX_VALUE : System.currentTimeMillis() + millis;

    // Crawlers can be replaced while this waits, so it looks again after each wait instead of walking the list once
    while (System.currentTimeMillis() < deadline) {
      Thread alive = threads.stream().filter(Thread::isAlive).findFirst().orElse(null);
      if (alive == null) {
        return;
      }
      alive.join(Math.min(JOIN_POLL_MILLIS, Math.max(1, deadline - System.currentTimeMillis())));
    }
  }
}
