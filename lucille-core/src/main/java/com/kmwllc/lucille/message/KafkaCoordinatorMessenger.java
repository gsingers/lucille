package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.WorkUnit;
import com.typesafe.config.Config;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CoordinatorMessenger that uses Kafka. Work units go to the work topic, spread evenly over its partitions.
 * Events are read from, and written to, the run's event topic.
 *
 * The event topic is read without a consumer group, either from its beginning, to replay a run that an earlier
 * Coordinator started, or from its current end.
 */
public class KafkaCoordinatorMessenger implements CoordinatorMessenger {

  // Events are read in batches because a Coordinator receives at least two per Document, and many more when replaying.
  private static final int EVENT_BATCH_SIZE = 500;

  private static final Logger log = LoggerFactory.getLogger(KafkaCoordinatorMessenger.class);

  private final Config config;
  private final CrawlConfig crawlConfig;
  private final boolean replay;
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  private final ArrayDeque<ConsumerRecord<String, String>> polledEvents = new ArrayDeque<>();

  private KafkaProducer<String, Document> documentProducer;
  private KafkaProducer<String, String> stringProducer;
  private Consumer<String, String> eventConsumer;
  private Map<TopicPartition, Long> replayEndOffsets = Map.of();
  private final AtomicInteger nextWorkPartition = new AtomicInteger();
  private int numWorkPartitions;
  private String runId;
  private String pipelineName;
  private String eventTopicName;

  /**
   * @param replay whether pollEvent() should start from the beginning of the run's event topic rather than its end.
   */
  public KafkaCoordinatorMessenger(Config config, boolean replay) {
    this.config = config;
    this.crawlConfig = new CrawlConfig(config);
    this.replay = replay;
  }

  @Override
  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId != null) {
      throw new Exception("Already initialized.");
    }
    this.runId = runId;
    this.pipelineName = pipelineName;
    this.eventTopicName = KafkaUtils.getEventTopicName(config, pipelineName, runId);

    KafkaUtils.createEventTopic(config, pipelineName, runId);
    KafkaUtils.createTopicIfAbsent(config,
        new NewTopic(crawlConfig.workTopic, crawlConfig.workTopicPartitions, crawlConfig.topicReplicationFactor));

    this.documentProducer = KafkaUtils.createDocumentProducer(config);
    this.stringProducer = KafkaUtils.createEventProducer(config);
    if (stringProducer == null) {
      throw new IllegalArgumentException("A distributed crawl requires Events; kafka.events cannot be false.");
    }

    // an existing work topic may have a different number of partitions than this config would create it with
    this.numWorkPartitions = stringProducer.partitionsFor(crawlConfig.workTopic).size();

    this.eventConsumer = KafkaUtils.createUngroupedConsumer(config, "com.kmwllc.lucille-coordinator-" + pipelineName,
        EVENT_BATCH_SIZE);
    List<TopicPartition> partitions = eventConsumer.partitionsFor(eventTopicName).stream()
        .map(info -> new TopicPartition(info.topic(), info.partition())).toList();
    eventConsumer.assign(partitions);

    if (replay) {
      replayEndOffsets = eventConsumer.endOffsets(partitions);
      eventConsumer.seekToBeginning(partitions);
    } else {
      eventConsumer.seekToEnd(partitions);
    }
    // seeks are lazy; asking for the position applies them now, before anything else is written to the topic
    for (TopicPartition partition : partitions) {
      eventConsumer.position(partition);
    }
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public void sendForProcessing(Document document) throws Exception {
    checkException();
    documentProducer.send(
        new ProducerRecord<>(KafkaUtils.getSourceTopicName(pipelineName, config), document.getId(), document),
        (metadata, exception) -> {
          if (exception != null) {
            log.error("Kafka send failed for document: {}", document.getId(), exception);
            sendException.compareAndSet(null, exception);
          }
        });
  }

  @Override
  public void flush() throws Exception {
    documentProducer.flush();
    // twice, because a unit is only handed to the producer once the send of its Event has completed
    stringProducer.flush();
    stringProducer.flush();
    checkException();
  }

  private void checkException() throws Exception {
    Exception e = sendException.get();
    if (e != null) {
      throw new Exception("Kafka send failed", e);
    }
  }

  @Override
  public void dispatchUnit(WorkUnit unit) throws Exception {
    // Units are dealt out to the partitions in turn. Choosing the partition from a hash of the key would leave some
    // partitions, and so some Crawlers, with several times the units of others when a run has few units.
    int partition = Math.floorMod(nextWorkPartition.getAndIncrement(), numWorkPartitions);
    stringProducer.send(new ProducerRecord<>(crawlConfig.workTopic, partition, unit.unitId(), unit.toJson())).get();
  }

  /**
   * Neither send is waited for, since waiting on two round trips per unit limits a run to a few dozen units a
   * second. The order is kept by sending the unit from the callback that confirms its Event was accepted.
   */
  @Override
  public void logAndDispatchUnit(Event unitCreated, WorkUnit unit) throws Exception {
    checkException();
    int partition = Math.floorMod(nextWorkPartition.getAndIncrement(), numWorkPartitions);
    ProducerRecord<String, String> workRecord =
        new ProducerRecord<>(crawlConfig.workTopic, partition, unit.unitId(), unit.toJson());

    stringProducer.send(new ProducerRecord<>(eventTopicName, unitCreated.getDocumentId(), unitCreated.toString()),
        (metadata, eventException) -> {
          if (eventException != null) {
            log.error("Kafka send failed for UNIT_CREATED event of unit: {}", unit.unitId(), eventException);
            sendException.compareAndSet(null, eventException);
            return;
          }

          stringProducer.send(workRecord, (workMetadata, workException) -> {
            if (workException != null) {
              log.error("Kafka send failed for unit: {}", unit.unitId(), workException);
              sendException.compareAndSet(null, workException);
            }
          });
        });
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    stringProducer.send(new ProducerRecord<>(eventTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public Event pollEvent() throws Exception {
    // a unit that could not be dispatched will never be reported done, so the run fails now instead of waiting for it
    checkException();

    if (polledEvents.isEmpty()) {
      eventConsumer.poll(KafkaUtils.POLL_INTERVAL).forEach(polledEvents::add);
    }

    // A record that cannot be read is skipped. If it were allowed to fail the run, the run could never be resumed
    // either, since every replay of the topic would meet the same record.
    ConsumerRecord<String, String> record;
    while ((record = polledEvents.poll()) != null) {
      try {
        return Event.fromJsonString(record.value());
      } catch (Exception e) {
        log.warn("Skipping unreadable event at offset {} of {}.", record.offset(), record.topic(), e);
      }
    }
    return null;
  }

  @Override
  public boolean replayComplete() {
    if (!polledEvents.isEmpty()) {
      return false;
    }

    for (Map.Entry<TopicPartition, Long> end : replayEndOffsets.entrySet()) {
      if (eventConsumer.position(end.getKey()) < end.getValue()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {
    closeQuietly(documentProducer, "document producer");
    closeQuietly(stringProducer, "event producer");
    closeQuietly(eventConsumer, "event consumer");
  }

  static void closeQuietly(AutoCloseable client, String description) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        log.error("Couldn't close kafka {}", description, e);
      }
    }
  }
}
