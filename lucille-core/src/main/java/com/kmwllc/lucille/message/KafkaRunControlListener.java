package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.RunControlTracker;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the control topic on a thread of its own and keeps a RunControlTracker up to date for the Crawlers of one
 * process. Reads the whole topic, without a consumer group, so every Crawler process sees every run.
 */
class KafkaRunControlListener implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(KafkaRunControlListener.class);

  private final Config config;
  private final CrawlConfig crawlConfig;
  private final RunControlTracker tracker;
  private final Thread thread;

  private volatile boolean running = true;
  private volatile KafkaConsumer<String, String> consumer;

  KafkaRunControlListener(Config config, CrawlConfig crawlConfig) {
    this.config = config;
    this.crawlConfig = crawlConfig;
    this.tracker = new RunControlTracker(TimeUnit.SECONDS.toMillis(crawlConfig.orphanTimeoutSecs));
    this.thread = new Thread(this, ThreadNameUtils.createName("RunControlListener"));
    this.thread.setDaemon(true);
  }

  RunControlTracker getTracker() {
    return tracker;
  }

  void start() {
    thread.start();
  }

  void stop() {
    running = false;
    KafkaConsumer<String, String> current = consumer;
    if (current != null) {
      current.wakeup();
    }
  }

  @Override
  public void run() {
    while (running) {
      try {
        listen();
      } catch (WakeupException e) {
        // stop() was called
      } catch (Exception e) {
        // Without heartbeats the Crawlers will come to treat every run as orphaned, so keep trying to reconnect.
        log.error("Error reading the control topic; will retry.", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
          return;
        }
      }
    }
  }

  private void listen() throws Exception {
    // a Crawler can be started before any Coordinator has created the topic
    KafkaUtils.createTopicIfAbsent(config, KafkaRunControl.newControlTopic(crawlConfig));

    try (KafkaConsumer<String, String> newConsumer =
        KafkaUtils.createUngroupedConsumer(config, "com.kmwllc.lucille-run-control-listener", 500)) {
      consumer = newConsumer;
      List<TopicPartition> partitions = List.of(new TopicPartition(crawlConfig.controlTopic, 0));
      newConsumer.assign(partitions);
      newConsumer.seekToBeginning(partitions);

      // Records that were already in the topic when this listener started may be old, so they are dated by when
      // they were written. Later ones are dated by when they arrive, which does not depend on the clock of the
      // Coordinator's host agreeing with this one.
      boolean caughtUp = false;
      long lastPrune = System.currentTimeMillis();

      while (running) {
        ConsumerRecords<String, String> records = newConsumer.poll(Duration.ofSeconds(1));
        long now = System.currentTimeMillis();

        for (ConsumerRecord<String, String> record : records) {
          apply(record, caughtUp ? now : Math.min(now, record.timestamp()));
        }

        if (records.isEmpty()) {
          caughtUp = true;
        }

        if (now - lastPrune > TimeUnit.MINUTES.toMillis(10)) {
          tracker.prune(TimeUnit.HOURS.toMillis(24));
          lastPrune = now;
        }
      }
    } finally {
      consumer = null;
    }
  }

  private void apply(ConsumerRecord<String, String> record, long seenAtMillis) {
    // no Coordinator writes a record under such a key, and the tracker should not be made to remember it
    if (!CrawlConfig.isValidRunId(record.key())) {
      return;
    }

    try {
      Event event = Event.fromJsonString(record.value());
      ObjectNode message = CrawlConfig.parseMessage(event.getMessage());
      int epoch = message.path(KafkaRunControl.EPOCH).asInt();

      if (Event.Type.CANCEL.equals(event.getType())) {
        tracker.onCancel(record.key(), epoch);
      } else if (Event.Type.HEARTBEAT.equals(event.getType())) {
        JsonNode allowance = message.path(KafkaRunControl.UNIT_CONCURRENCY);
        tracker.onHeartbeat(record.key(), epoch, seenAtMillis, allowance.isInt() && allowance.asInt() > 0 ? allowance.asInt() : null);
      }
    } catch (Exception e) {
      log.warn("Ignoring unreadable record on control topic at offset {}.", record.offset(), e);
    }
  }
}
