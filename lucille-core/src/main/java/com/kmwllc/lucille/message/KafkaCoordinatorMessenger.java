package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.WorkUnit;
import com.typesafe.config.Config;
import com.kmwllc.lucille.util.ThreadNameUtils;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
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

  // How long to wait for a topic that has just been created to become visible. Kafka's default.api.timeout.ms.
  private static final long TOPIC_WAIT_MILLIS = 60_000;
  private static final long TOPIC_RETRY_MILLIS = 100;

  private static final Logger log = LoggerFactory.getLogger(KafkaCoordinatorMessenger.class);

  private final Config config;
  private final CrawlConfig crawlConfig;
  private final boolean replay;
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  private final ArrayDeque<ConsumerRecord<String, String>> polledEvents = new ArrayDeque<>();

  private Producer<String, Document> documentProducer;
  private Producer<String, String> stringProducer;
  private Consumer<String, String> eventConsumer;
  // where the event topic ended when a replay began; null until then, and always if no replay was asked for
  private Map<TopicPartition, Long> replayEndOffsets;
  private int numWorkPartitions;
  // Sends work records on behalf of the callbacks that confirm their Events. A producer callback runs on the
  // producer's I/O thread, and a send made there deadlocks if it has to wait for metadata, which only that thread
  // can fetch: a work topic that has had no sends for metadata.max.idle.ms needs exactly that.
  private final ExecutorService dispatcher = Executors.newSingleThreadExecutor(
      new BasicThreadFactory.Builder().namingPattern(ThreadNameUtils.createName("CoordinatorDispatcher")).daemon(true).build());
  // Events this messenger makes itself, about units it could not log or dispatch; returned by pollEvent() first
  private final ConcurrentLinkedQueue<Event> localEvents = new ConcurrentLinkedQueue<>();
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

    // One partition, so that the Events of a run are read in the order they were written. The topic is the only
    // record of what the run has done, so it is replicated like the other topics of a crawl; KafkaUtils.createEventTopic,
    // which the other run types use, always creates a single replica.
    KafkaUtils.createTopicIfAbsent(config, crawlConfig.newTopic(eventTopicName, 1));
    KafkaUtils.createTopicIfAbsent(config, crawlConfig.newTopic(crawlConfig.workTopic, crawlConfig.workTopicPartitions));

    this.documentProducer = KafkaUtils.createDocumentProducer(config);
    this.stringProducer = KafkaUtils.createEventProducer(config);
    if (stringProducer == null) {
      throw new IllegalArgumentException("A distributed crawl requires Events; kafka.events cannot be false.");
    }

    // an existing work topic may have a different number of partitions than this config would create it with
    this.numWorkPartitions = stringProducer.partitionsFor(crawlConfig.workTopic).size();

    assignEventConsumer(KafkaUtils.createUngroupedConsumer(config, "com.kmwllc.lucille-coordinator-" + pipelineName,
        EVENT_BATCH_SIZE), TOPIC_WAIT_MILLIS);
  }

  // package access so unit tests can supply a consumer
  void assignEventConsumer(Consumer<String, String> consumer, long topicWaitMillis) throws Exception {
    this.eventConsumer = consumer;
    List<TopicPartition> partitions = awaitPartitions(topicWaitMillis);
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

  /**
   * Returns the partitions of the event topic, waiting for the topic to become visible if need be. Creating a topic
   * returns once the cluster has accepted it, which can be before every broker knows of it, and a broker that does
   * not know of it yet reports no partitions. Carrying on with none would leave the consumer assigned to nothing.
   */
  private List<TopicPartition> awaitPartitions(long topicWaitMillis) throws Exception {
    long deadline = System.currentTimeMillis() + topicWaitMillis;

    while (true) {
      List<PartitionInfo> partitions = eventConsumer.partitionsFor(eventTopicName);

      if (partitions != null && !partitions.isEmpty()) {
        return partitions.stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
      }
      if (System.currentTimeMillis() >= deadline) {
        throw new Exception("Event topic " + eventTopicName + " had no partitions visible to this consumer after "
            + topicWaitMillis + " ms.");
      }

      log.info("Waiting for event topic {} to become visible.", eventTopicName);
      Thread.sleep(TOPIC_RETRY_MILLIS);
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
    // A unit is handed to the dispatcher once the send of its Event has completed, and to the producer once the
    // dispatcher gets to it. So: complete the Events, let the dispatcher catch up, then complete the units.
    stringProducer.flush();
    dispatcher.submit(() -> { }).get();
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
  public int numWorkPartitions() {
    return numWorkPartitions;
  }

  @Override
  public void dispatchUnit(WorkUnit unit, int partition) throws Exception {
    stringProducer.send(workRecord(unit, partition)).get();
  }

  private ProducerRecord<String, String> workRecord(WorkUnit unit, int partition) {
    return new ProducerRecord<>(crawlConfig.workTopic, partition, unit.unitId(), unit.toJson());
  }

  /**
   * Neither send is waited for, since waiting on two round trips per unit limits a run to a few dozen units a
   * second. The order is kept by handing the unit to the dispatcher from the callback that confirms its Event was
   * accepted. If either send fails, the unit is reported as failed, so that the Coordinator tries it again.
   */
  @Override
  public void logAndDispatchUnit(Event unitCreated, WorkUnit unit, int partition) throws Exception {
    checkException();
    ProducerRecord<String, String> workRecord = workRecord(unit, partition);

    stringProducer.send(new ProducerRecord<>(eventTopicName, unitCreated.getDocumentId(), unitCreated.toString()),
        (metadata, eventException) -> {
          if (eventException != null) {
            reportDispatchFailure(unit, "Its UNIT_CREATED event could not be written", eventException);
            return;
          }

          try {
            dispatcher.execute(() -> {
              try {
                stringProducer.send(workRecord, (workMetadata, workException) -> {
                  if (workException != null) {
                    reportDispatchFailure(unit, "It could not be sent to the work topic", workException);
                  }
                });
              } catch (Exception e) {
                // thrown rather than passed to the callback: the producer is closed, or the thread interrupted
                reportDispatchFailure(unit, "It could not be sent to the work topic", e);
              }
            });
          } catch (RejectedExecutionException e) {
            // closed; the Coordinator is on its way out and whatever resumes the run dispatches the unit again
            log.warn("Not dispatching unit {}: the messenger is closed.", unit.unitId());
          }
        });
  }

  // The failure is the unit's, not the run's: the Coordinator handles the report as it would one from a Crawler, and
  // dispatches the unit again until crawl.maxAttempts says otherwise.
  private void reportDispatchFailure(WorkUnit unit, String what, Exception cause) {
    log.error("{}: unit {} (attempt {}). It will be dispatched again.", what, unit.unitId(), unit.attempt(), cause);
    localEvents.add(unit.reportEvent(Event.Type.UNIT_FAILED, "coordinator", what + ": " + cause));
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    stringProducer.send(new ProducerRecord<>(eventTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public Event pollEvent() throws Exception {
    checkException();

    // the messenger's own reports first; they are not in the log, and a unit they are about is only in memory
    Event local = localEvents.poll();
    if (local != null) {
      return local;
    }

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
    if (!replay) {
      return true;
    }

    // Until the consumer has been attached to the topic, nothing is known about what there is to replay. Reporting
    // the replay complete then would let a resumed run carry on as though it had no history.
    if (replayEndOffsets == null || !polledEvents.isEmpty()) {
      return false;
    }

    for (Map.Entry<TopicPartition, Long> end : replayEndOffsets.entrySet()) {
      if (eventConsumer.position(end.getKey()) < end.getValue()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Reads the given run's event topic from the beginning and returns the source calls each unit's accepted
   * completion reported. A unit done more than once keeps the largest figure.
   */
  @Override
  public Map<String, Long> readUnitCosts(String costsRunId, String costsPipelineName) throws Exception {
    if (!CrawlConfig.isValidRunId(costsRunId)) {
      throw new IllegalArgumentException("Run ID " + costsRunId + " cannot be used.");
    }
    String topic = KafkaUtils.getEventTopicName(config, costsPipelineName, costsRunId);
    Map<String, Long> costs = new HashMap<>();

    try (Consumer<String, String> consumer = KafkaUtils.createUngroupedConsumer(config,
        "com.kmwllc.lucille-costs-" + costsPipelineName, EVENT_BATCH_SIZE)) {
      // asked of the broker's list rather than the topic, which a metadata request for a name could create
      if (!consumer.listTopics().containsKey(topic)) {
        log.warn("Run {} has no event topic to read unit costs from; units will be dispatched in planning order.", costsRunId);
        return costs;
      }
      List<PartitionInfo> info = consumer.partitionsFor(topic);
      if (info == null || info.isEmpty()) {
        log.warn("Run {} has no event topic to read unit costs from; units will be dispatched in planning order.", costsRunId);
        return costs;
      }

      List<TopicPartition> partitions = info.stream().map(i -> new TopicPartition(i.topic(), i.partition())).toList();
      consumer.assign(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      consumer.seekToBeginning(partitions);

      while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
        for (ConsumerRecord<String, String> record : consumer.poll(KafkaUtils.POLL_INTERVAL)) {
          try {
            Event event = Event.fromJsonString(record.value());
            if (event.getType() == Event.Type.UNIT_DONE && event.getDocumentId() != null) {
              long calls = CrawlConfig.parseMessage(event.getMessage()).path("sourceCalls").asLong(0);
              costs.merge(event.getDocumentId(), calls, Math::max);
            }
          } catch (Exception e) {
            log.debug("Skipping unreadable event at offset {} of {}.", record.offset(), topic);
          }
        }
      }
    }

    log.info("Read the cost of {} units from run {}.", costs.size(), costsRunId);
    return costs;
  }

  @Override
  public void close() {
    dispatcher.shutdown();
    try {
      dispatcher.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
