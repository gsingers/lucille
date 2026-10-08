package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.RunControlTracker;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CrawlerMessenger that uses Kafka.
 *
 * Work units are received from the work topic as a member of the Crawlers' consumer group. Receiving a unit is
 * claiming it: its offset is committed only when the unit is acknowledged, so a unit whose Crawler dies is delivered
 * to another member of the group.
 *
 * A unit can take far longer to execute than <code>max.poll.interval.ms</code>, and a consumer that does not poll
 * for that long is removed from its group. So while a unit is held, the consumer is paused and a separate thread
 * keeps polling it. A paused consumer returns no records but remains a live member of the group.
 *
 * Documents are sent without waiting for each to be accepted. The CREATE Event for a Document is sent only after the
 * Document has been accepted, so the Coordinator is never told of a Document that did not reach the source topic.
 * See {@link CrawlerProducers} for which producer sends what.
 */
public class KafkaCrawlerMessenger implements CrawlerMessenger {

  private static final long KEEP_ALIVE_PERIOD_MILLIS = 1000;
  private static final long COMMIT_TIMEOUT_MILLIS = 30_000;

  private static final Logger log = LoggerFactory.getLogger(KafkaCrawlerMessenger.class);

  private final Config config;
  private final RunControlTracker tracker;
  private final CrawlerProducers producers;
  private final ScheduledExecutorService keepAlive;

  // A KafkaConsumer must not be used by two threads at once. Guards the consumer and the fields describing the held unit.
  private final Object consumerLock = new Object();
  private final KafkaConsumer<String, String> workConsumer;
  private ConsumerRecord<String, String> heldRecord;
  private volatile boolean unitLost;

  public KafkaCrawlerMessenger(Config config, CrawlConfig crawlConfig, RunControlTracker tracker) {
    this.config = config;
    this.tracker = tracker;
    this.producers = CrawlerProducers.create(config, crawlConfig.documentProducers);

    try {
      // a Crawler can be started before any Coordinator has created the topic
      KafkaUtils.createTopicIfAbsent(config, crawlConfig.newTopic(crawlConfig.workTopic, crawlConfig.workTopicPartitions));
    } catch (Exception e) {
      throw new IllegalStateException("Could not create work topic " + crawlConfig.workTopic, e);
    }

    this.workConsumer = new KafkaConsumer<>(createWorkConsumerProps(config, crawlConfig));
    this.workConsumer.subscribe(Collections.singletonList(crawlConfig.workTopic), new RebalanceListener());

    BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("CrawlerKeepAlive")).daemon(true).build();
    this.keepAlive = Executors.newSingleThreadScheduledExecutor(threadFactory);
    this.keepAlive.scheduleWithFixedDelay(this::pollWhileHolding, KEEP_ALIVE_PERIOD_MILLIS, KEEP_ALIVE_PERIOD_MILLIS,
        TimeUnit.MILLISECONDS);
  }

  // package access so unit tests can validate the properties without initializing a Consumer
  static Properties createWorkConsumerProps(Config config, CrawlConfig crawlConfig) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate crawler threads
    String clientId = "com.kmwllc.lucille-crawler-" + RandomStringUtils.randomAlphanumeric(8);
    Properties props = KafkaUtils.createConsumerProps(config, clientId);

    // Crawlers must not share a group with Workers or Indexers: a member joining or leaving any role would then
    // rebalance all of them.
    props.put(ConsumerConfig.GROUP_ID_CONFIG, crawlConfig.consumerGroupId);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    // When a Crawler joins or leaves, only the partitions that move are taken from their owners. With the default
    // assignor every Crawler would give up its partition, and with it the unit it is executing.
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    return props;
  }

  @Override
  public WorkUnit pollWorkUnit() throws Exception {
    synchronized (consumerLock) {
      if (heldRecord != null) {
        throw new IllegalStateException("A work unit is already held; it must be acknowledged first.");
      }

      ConsumerRecords<String, String> records = workConsumer.poll(KafkaUtils.getPollInterval(config));
      KafkaUtils.validateAtMostOneRecord(records);
      if (records.isEmpty()) {
        return null;
      }

      ConsumerRecord<String, String> record = records.iterator().next();
      WorkUnit unit;
      try {
        unit = WorkUnit.fromJson(record.value());
      } catch (Exception e) {
        // an unreadable record would otherwise be redelivered forever
        log.error("Discarding unreadable work unit at {}-{} offset {}.", record.topic(), record.partition(), record.offset(), e);
        workConsumer.commitSync(offsetAfter(record));
        return null;
      }

      // A unit that failed or was abandoned can leave sends in flight. They are allowed to finish first, so that
      // their outcome is not taken for this unit's.
      producers.startUnit();
      heldRecord = record;
      unitLost = false;
      workConsumer.pause(workConsumer.assignment());
      return unit;
    }
  }

  private static Map<TopicPartition, OffsetAndMetadata> offsetAfter(ConsumerRecord<String, String> record) {
    return Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
  }

  /**
   * Keeps the consumer in its group while a unit is held. Does nothing otherwise, as the Crawler is then polling.
   */
  private void pollWhileHolding() {
    synchronized (consumerLock) {
      if (heldRecord == null) {
        return;
      }

      try {
        // Partitions assigned by a rebalance during this call are paused by the RebalanceListener. The poll is given a
        // little time because a rebalance of the group cannot finish until every member has taken part in it.
        ConsumerRecords<String, String> records = workConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          // not expected while paused; put the record back so that it is delivered once the held unit is acknowledged
          workConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        }
      } catch (Exception e) {
        log.error("Error polling to keep the Crawler in its consumer group.", e);
      }
    }
  }

  @Override
  public void ackWorkUnit() {
    synchronized (consumerLock) {
      if (heldRecord == null) {
        throw new IllegalStateException("No work unit is held.");
      }

      try {
        if (unitLost) {
          log.warn("Not committing the held unit: its partition was reassigned, so it will be delivered again.");
        } else {
          commitHeldRecord();
        }
      } finally {
        heldRecord = null;
        workConsumer.resume(workConsumer.assignment());
      }
    }
  }

  // A commit that fails leaves the unit to be delivered again, which the Coordinator tolerates, so it is not an error.
  private void commitHeldRecord() {
    long deadline = System.currentTimeMillis() + COMMIT_TIMEOUT_MILLIS;

    while (!unitLost && System.currentTimeMillis() < deadline) {
      try {
        workConsumer.commitSync(offsetAfter(heldRecord));
        return;
      } catch (RebalanceInProgressException e) {
        // The commit can succeed once the rebalance is over, provided the unit's partition stays with this consumer.
        // Polling is what moves the rebalance along; the consumer is paused, so no record is returned.
        workConsumer.poll(Duration.ofMillis(200));
      } catch (Exception e) {
        log.warn("Could not commit the held unit; it will be delivered again.", e);
        return;
      }
    }
    log.warn("Could not commit the held unit during a rebalance; it will be delivered again.");
  }

  @Override
  public void releaseWorkUnit() {
    synchronized (consumerLock) {
      if (heldRecord == null) {
        return;
      }

      try {
        TopicPartition partition = new TopicPartition(heldRecord.topic(), heldRecord.partition());
        if (!unitLost && workConsumer.assignment().contains(partition)) {
          workConsumer.seek(partition, heldRecord.offset());
        }
      } finally {
        heldRecord = null;
        workConsumer.resume(workConsumer.assignment());
      }
    }
  }

  @Override
  public boolean isUnitLost() {
    return unitLost;
  }

  @Override
  public void sendForProcessing(Document document, String pipelineName) throws Exception {
    producers.sendForProcessing(document, pipelineName);
  }

  @Override
  public void flush(String pipelineName) throws Exception {
    producers.flush(pipelineName);
  }

  @Override
  public void sendEvent(Event event, String pipelineName) throws Exception {
    producers.sendEvent(event, pipelineName);
  }

  @Override
  public RunControlTracker getRunControlTracker() {
    return tracker;
  }

  @Override
  public void close() {
    keepAlive.shutdownNow();
    synchronized (consumerLock) {
      KafkaCoordinatorMessenger.closeQuietly(workConsumer, "work consumer");
    }
    producers.close();
  }

  /**
   * Called from within poll(), on whichever thread is polling, with consumerLock held.
   */
  private class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      if (heldRecord != null) {
        workConsumer.pause(partitions);
      }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      if (heldRecord != null && partitions.contains(new TopicPartition(heldRecord.topic(), heldRecord.partition()))) {
        unitLost = true;
      }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      onPartitionsRevoked(partitions);
    }
  }
}
