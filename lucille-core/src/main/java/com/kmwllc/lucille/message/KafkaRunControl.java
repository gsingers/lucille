package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;

/**
 * A RunControl that writes to the control topic: a single-partition, compacted Kafka topic keyed by run ID, so that
 * the most recent record for every run is retained and a Crawler that starts later still learns of it.
 */
public class KafkaRunControl implements RunControl {

  static final String EPOCH = "epoch";
  static final String CONFIG_HASH = "configHash";
  static final String REASON = "reason";

  private final Config config;
  private final String controlTopic;
  private final KafkaProducer<String, String> producer;

  public KafkaRunControl(Config config) throws Exception {
    CrawlConfig crawlConfig = new CrawlConfig(config);
    this.config = config;
    this.controlTopic = crawlConfig.controlTopic;

    KafkaUtils.createTopicIfAbsent(config, newControlTopic(crawlConfig));

    this.producer = KafkaUtils.createEventProducer(config);
    if (producer == null) {
      throw new IllegalArgumentException("A distributed crawl requires Events; kafka.events cannot be false.");
    }
  }

  /**
   * Describes the control topic. Segments are rolled often so that compaction, which skips the newest segment, keeps
   * the topic down to roughly one record per run.
   */
  static NewTopic newControlTopic(CrawlConfig crawlConfig) {
    return new NewTopic(crawlConfig.controlTopic, 1, crawlConfig.topicReplicationFactor)
        .configs(Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
            TopicConfig.SEGMENT_MS_CONFIG, "600000",
            TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1"));
  }

  @Override
  public Status latest(String runId) throws Exception {
    ConsumerRecord<String, String> latest = null;

    try (KafkaConsumer<String, String> consumer =
        KafkaUtils.createUngroupedConsumer(config, "com.kmwllc.lucille-run-control-reader", 500)) {
      List<TopicPartition> partitions = List.of(new TopicPartition(controlTopic, 0));
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      long end = consumer.endOffsets(partitions).get(partitions.get(0));

      while (consumer.position(partitions.get(0)) < end) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
          if (runId.equals(record.key())) {
            latest = record;
          }
        }
      }
    }

    if (latest == null) {
      return null;
    }

    Event event = Event.fromJsonString(latest.value());
    ObjectNode message = CrawlConfig.parseMessage(event.getMessage());
    return new Status(Event.Type.CANCEL.equals(event.getType()), message.path(EPOCH).asInt(),
        message.path(CONFIG_HASH).asText(), System.currentTimeMillis() - latest.timestamp());
  }

  @Override
  public void heartbeat(String runId, int epoch, String configHash) throws Exception {
    send(runId, CrawlConfig.newMessage().put(EPOCH, epoch).put(CONFIG_HASH, configHash), Event.Type.HEARTBEAT);
  }

  @Override
  public void cancel(String runId, int epoch, String configHash, String reason) throws Exception {
    ObjectNode message = CrawlConfig.newMessage().put(EPOCH, epoch).put(CONFIG_HASH, configHash).put(REASON, reason);
    send(runId, message, Event.Type.CANCEL);
  }

  private void send(String runId, ObjectNode message, Event.Type type) throws Exception {
    Event event = new Event(runId, runId, message.toString(), type);
    producer.send(new ProducerRecord<>(controlTopic, runId, event.toString())).get();
  }

  @Override
  public void close() {
    KafkaCoordinatorMessenger.closeQuietly(producer, "control producer");
  }
}
