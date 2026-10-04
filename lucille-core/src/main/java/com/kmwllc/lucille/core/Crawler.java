package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.RunControlTracker.Decision;
import com.kmwllc.lucille.message.CrawlerMessenger;
import com.kmwllc.lucille.message.CrawlerMessengerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import sun.misc.Signal;

/**
 * Executes the work units of distributed crawls. A Crawler stays alive indefinitely, polling for units from any run.
 * For each unit it instantiates the connector the unit names from its own config, has that connector publish the
 * unit's Documents, and reports to the run's Coordinator whether the unit succeeded.
 */
class Crawler implements Runnable {

  public static final String UNIT_ID_FIELD = "unit_id";
  public static final String METRICS_PREFIX = "crawler";

  private static final Logger log = LoggerFactory.getLogger(Crawler.class);
  private static final long ERROR_PAUSE_MILLIS = 1000;

  private final Config config;
  private final CrawlerMessenger messenger;
  private final String crawlerName;
  private final long orphanTimeoutMillis;

  private final Timer unitTimer;
  private final com.codahale.metrics.Meter docMeter;
  private final com.codahale.metrics.Counter abandonedUnits;

  private volatile boolean running = true;

  // The connector for the most recent unit, kept so that consecutive units of the same connector and run share its
  // connections. Closed when a unit for another connector or run arrives.
  private PartitionableConnector cachedConnector;
  private String cachedConnectorKey;
  private String cachedConfigHash;

  Crawler(Config config, CrawlerMessenger messenger, String crawlerName) {
    this.config = config;
    this.messenger = messenger;
    this.crawlerName = crawlerName;
    this.orphanTimeoutMillis = TimeUnit.SECONDS.toMillis(new CrawlConfig(config).orphanTimeoutSecs);

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.unitTimer = metrics.timer(METRICS_PREFIX + ".unit.duration");
    this.docMeter = metrics.meter(METRICS_PREFIX + ".docs.published");
    this.abandonedUnits = metrics.counter(METRICS_PREFIX + ".units.abandoned");
  }

  public void terminate() {
    log.debug("terminate called");
    running = false;
  }

  @Override
  public void run() {
    try {
      pollForUnits();
    } finally {
      // Closing the messenger gives up any unit still held. This must happen however the thread ends: a unit held by
      // a thread that has died would otherwise never be delivered to another Crawler.
      closeCachedConnector();
      messenger.close();
      log.debug("Exiting");
    }
  }

  private void pollForUnits() {
    while (running) {
      WorkUnit unit;
      try {
        unit = messenger.pollWorkUnit();
      } catch (Exception e) {
        log.error("Crawler could not poll for a work unit; will retry.", e);
        pauseAfterError();
        continue;
      }

      if (unit == null) {
        continue;
      }

      MDC.put(RUNID_FIELD, unit.runId());
      MDC.put(UNIT_ID_FIELD, unit.unitId());
      try {
        handleUnit(unit);
      } catch (Exception e) {
        log.error("Crawler could not report on unit {}; it will be delivered again.", unit.unitId(), e);
        messenger.releaseWorkUnit();
        pauseAfterError();
      } finally {
        MDC.remove(RUNID_FIELD);
        MDC.remove(UNIT_ID_FIELD);
      }
    }
  }

  private void pauseAfterError() {
    try {
      Thread.sleep(ERROR_PAUSE_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running = false;
    }
  }

  /**
   * Executes the unit if its run is alive, tells the Coordinator how it went, and acknowledges it.
   */
  private void handleUnit(WorkUnit unit) throws Exception {
    Decision decision = messenger.getRunControlTracker().awaitDecision(unit.runId(), unit.epoch(), orphanTimeoutMillis);

    if (decision != Decision.RUN) {
      abandon(unit, decision);
      return;
    }

    log.info("Executing unit {} (attempt {}).", unit.unitId(), unit.attempt());
    Timer.Context timing = unitTimer.time();
    CrawlerPublisher publisher = null;
    String error = null;

    try {
      PartitionableConnector connector = connectorFor(unit);
      publisher = new CrawlerPublisher(messenger, unit, connector.getPipelineName(), connector.requiresCollapsingPublisher());
      connector.executeUnit(unit, publisher);
      publisher.flush();
    } catch (Exception e) {
      log.error("Unit {} failed.", unit.unitId(), e);
      error = String.valueOf(e.getMessage());
      // a connector that failed part way through may be in no state to execute another unit
      closeCachedConnector();
    }

    long durationMillis = TimeUnit.NANOSECONDS.toMillis(timing.stop());
    long numPublished = publisher == null ? 0 : publisher.numPublished();
    docMeter.mark(numPublished);

    if (publisher != null && publisher.isUnitLost()) {
      log.warn("Unit {} was reassigned while executing; another Crawler will execute it.", unit.unitId());
      abandonedUnits.inc();
    } else if (publisher != null && publisher.getAbortReason() != null) {
      abandon(unit, publisher.getAbortReason());
      return;
    } else {
      ObjectNode message = unitMessage(unit).put("numPublished", numPublished).put("durationMs", durationMillis);
      if (error == null) {
        sendUnitEvent(unit, message, Event.Type.UNIT_DONE);
        log.info("Unit {} done: {} docs in {} ms.", unit.unitId(), numPublished, durationMillis);
      } else {
        sendUnitEvent(unit, message.put("error", error), Event.Type.UNIT_FAILED);
      }
    }

    messenger.ackWorkUnit();
  }

  /**
   * Gives up on a unit that should not be, or should no longer be, executed. The Coordinator of an orphaned run may
   * in fact be alive and merely slow to send heartbeats, so it is told that the unit was not completed; a run that
   * was cancelled or taken over by a newer epoch is not waiting to hear about the unit.
   */
  private void abandon(WorkUnit unit, Decision reason) throws Exception {
    log.info("Abandoning unit {}: run is {}.", unit.unitId(), reason);
    abandonedUnits.inc();

    if (reason == Decision.ORPHANED) {
      ObjectNode message = unitMessage(unit).put("error", "Abandoned: no heartbeat from the run's Coordinator.");
      sendUnitEvent(unit, message, Event.Type.UNIT_FAILED);
    }

    messenger.ackWorkUnit();
  }

  private ObjectNode unitMessage(WorkUnit unit) {
    return CrawlConfig.newMessage().put("attempt", unit.attempt()).put("epoch", unit.epoch()).put("crawler", crawlerName);
  }

  private void sendUnitEvent(WorkUnit unit, ObjectNode message, Event.Type type) throws Exception {
    messenger.sendEvent(new Event(unit.unitId(), unit.runId(), message.toString(), type), unit.pipelineName());
  }

  /**
   * Returns a connector for the unit, built from this Crawler's own config. Only the connector's name is taken from
   * the unit; the class to instantiate and its settings are not.
   */
  private PartitionableConnector connectorFor(WorkUnit unit) throws Exception {
    String key = unit.runId() + "/" + unit.connectorName();

    if (!key.equals(cachedConnectorKey)) {
      closeCachedConnector();

      Config connectorConfig = CrawlConfig.connectorConfig(config, unit.connectorName());
      if (connectorConfig == null) {
        throw new ConnectorException("No connector named " + unit.connectorName() + " in this Crawler's config.");
      }

      Class<?> clazz = Class.forName(connectorConfig.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      cachedConnector = SingleUnitAdapter.wrap((Connector) constructor.newInstance(connectorConfig));
      cachedConnectorKey = key;
      cachedConfigHash = CrawlConfig.connectorConfigHash(connectorConfig);
    }

    if (!cachedConfigHash.equals(unit.configHash())) {
      throw new ConnectorException("Config for connector " + unit.connectorName()
          + " differs between this Crawler and the run's Coordinator.");
    }

    return cachedConnector;
  }

  private void closeCachedConnector() {
    if (cachedConnector != null) {
      try {
        cachedConnector.close();
      } catch (Exception e) {
        log.error("Error closing connector", e);
      }
    }
    cachedConnector = null;
    cachedConnectorKey = null;
    cachedConfigHash = null;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    CrawlerPool crawlerPool = new CrawlerPool(config, CrawlerMessengerFactory.getKafkaFactory(config));
    crawlerPool.start();

    Signal.handle(new Signal("INT"), signal -> {
      crawlerPool.stop();
      log.info("Crawlers shutting down");
      try {
        crawlerPool.join();
      } catch (InterruptedException e) {
        log.error("Interrupted", e);
      }
      System.exit(0);
    });
  }
}
