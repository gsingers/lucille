package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger log = LoggerFactory.getLogger(KafkaRunControl.class);

  private final Config config;
  private final String controlTopic;
  private final KafkaProducer<String, String> producer;

  // What has been read from the control topic so far, by run ID. Kept between calls so that each call only reads
  // what has been added since. Each Status holds the time its record was written in place of its age.
  private KafkaConsumer<String, String> reader;
  private final Map<String, Status> latestRecords = new TreeMap<>();

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
   * the topic down to roughly one record per run. Records are also deleted after a week, so that the topic does not
   * keep a record of every run there has ever been; a run that has been silent for that long can no longer be resumed.
   */
  static NewTopic newControlTopic(CrawlConfig crawlConfig) {
    return crawlConfig.newTopic(crawlConfig.controlTopic, 1)
        .configs(Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE,
            TopicConfig.RETENTION_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(7)),
            TopicConfig.SEGMENT_MS_CONFIG, "600000",
            TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1"));
  }

  /**
   * The record that counts for a run is the one from the highest epoch, so a Coordinator that has been superseded
   * cannot make the run appear to be its own again; within an epoch, a cancellation is final.
   */
  @Override
  public synchronized Status latest(String runId) throws Exception {
    readNewRecords();
    return withAge(latestRecords.get(runId));
  }

  @Override
  public synchronized Map<String, Status> list() throws Exception {
    readNewRecords();
    Map<String, Status> statuses = new TreeMap<>();
    latestRecords.forEach((runId, status) -> statuses.put(runId, withAge(status)));
    return statuses;
  }

  private static Status withAge(Status status) {
    return status == null ? null : new Status(status.cancelled(), status.epoch(), status.configHash(),
        System.currentTimeMillis() - status.ageMillis(), status.reason());
  }

  // Reads whatever has been added to the control topic since the last call.
  private void readNewRecords() {
    TopicPartition partition = new TopicPartition(controlTopic, 0);

    if (reader == null) {
      reader = KafkaUtils.createUngroupedConsumer(config, "com.kmwllc.lucille-run-control-reader", 500);
      reader.assign(List.of(partition));
      reader.seekToBeginning(List.of(partition));
    }

    long end = reader.endOffsets(List.of(partition)).get(partition);
    while (reader.position(partition) < end) {
      reader.poll(Duration.ofSeconds(1)).forEach(this::apply);
    }
  }

  private void apply(ConsumerRecord<String, String> record) {
    Status status;
    try {
      Event event = Event.fromJsonString(record.value());
      ObjectNode message = CrawlConfig.parseMessage(event.getMessage());
      boolean cancelled = Event.Type.CANCEL.equals(event.getType());
      status = new Status(cancelled, message.path(EPOCH).asInt(), message.path(CONFIG_HASH).asText(), record.timestamp(),
          cancelled ? message.path(REASON).asText(null) : null);
    } catch (Exception e) {
      log.warn("Ignoring unreadable record on control topic at offset {}.", record.offset(), e);
      return;
    }

    if (!CrawlConfig.isValidRunId(record.key())) {
      return;
    }

    latestRecords.merge(record.key(), status, (current, update) ->
        update.epoch() > current.epoch() || (update.epoch() == current.epoch() && !current.cancelled()) ? update : current);
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
  public synchronized void close() {
    KafkaCoordinatorMessenger.closeQuietly(producer, "control producer");
    KafkaCoordinatorMessenger.closeQuietly(reader, "control reader");
  }
}
