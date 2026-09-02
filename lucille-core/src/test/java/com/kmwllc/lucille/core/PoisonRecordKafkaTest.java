package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.message.KafkaWorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * DEMONSTRATION of the "K2" poison-pill defect. These tests PASS by asserting the CURRENT,
 * BROKEN behavior; they exist to document the failure mode, not to verify desired behavior.
 * A future fix (e.g. skipping/dead-lettering undeserializable records) should FLIP these
 * assertions.
 *
 * <p>The defect: {@code KafkaDocumentDeserializer.deserialize} throws a
 * {@code SerializationException} on any unparseable value. Inside {@code KafkaConsumer.poll}
 * this surfaces as a {@code RecordDeserializationException}, which propagates out of
 * {@code KafkaWorkerMessenger.pollDocToProcess}. {@code Worker.run} catches any Exception
 * from the poll, logs "interrupted" at INFO, calls {@code terminate()}, and returns --
 * the worker thread silently dies. Because the consumer position never advances past the
 * poison record, the partition is permanently stuck: valid records behind the poison record
 * are never consumed, and a restarted worker dies again on the same record.
 */
public class PoisonRecordKafkaTest {

  // This is a class-level Embedded instance of Kafka. Each test must use its own unique
  // topic names and consumer group ids to avoid conflicts. (Topic names are derived from
  // the pipeline name, so each test uses a unique pipeline name.)
  public static EmbeddedKafkaBroker embeddedKafka;

  @BeforeClass
  public static void startKafka() throws Exception {
    embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1);
    embeddedKafka.afterPropertiesSet();
  }

  @AfterClass
  public static void stopKafka() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  private static Config buildConfig(String pipelineName, String groupId) {
    return ConfigFactory.parseString(String.format(
        "kafka {\n"
            + "  bootstrapServers: \"%s\"\n"
            + "  consumerGroupId: \"%s\"\n"
            + "  maxPollIntervalSecs: 30\n"
            + "  maxRequestSize: 10000000\n"
            + "}\n"
            + "pipelines: [{name: \"%s\", stages: [{class: \"com.kmwllc.lucille.stage.NopStage\"}]}]\n",
        embeddedKafka.getBrokersAsString(), groupId, pipelineName));
  }

  /**
   * Produces three records to the (single-partition) source topic:
   * offset 0 = a valid serialized Document, offset 1 = raw garbage bytes that cannot be
   * deserialized (the poison record), offset 2 = another valid Document.
   */
  private void produceValidPoisonValid(String topic) throws Exception {
    Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      producer.send(new ProducerRecord<>(topic, "doc1",
          Document.create("doc1", "run1").toString().getBytes(StandardCharsets.UTF_8))).get();
      producer.send(new ProducerRecord<>(topic, "poison",
          new byte[]{0x00, 0x01, 0x02, 0x03})).get();
      producer.send(new ProducerRecord<>(topic, "doc3",
          Document.create("doc3", "run1").toString().getBytes(StandardCharsets.UTF_8))).get();
    }
  }

  /**
   * Demonstrates the defect at the messenger level: the first valid document is returned,
   * then every subsequent poll throws RecordDeserializationException for the poison record
   * at offset 1. The consumer never advances past it, so the valid document at offset 2 is
   * unreachable.
   */
  @Test(timeout = 120000)
  public void testPollThrowsRepeatedlyOnPoisonRecord() throws Exception {
    String pipelineName = "poison_direct";
    Config config = buildConfig(pipelineName, "poison_direct_group");
    String topic = KafkaUtils.getSourceTopicName(pipelineName, config);
    embeddedKafka.addTopics(new NewTopic(topic, 1, (short) 1));
    produceValidPoisonValid(topic);

    KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, pipelineName);
    try {
      // the first (valid) document comes through normally
      Document first = null;
      long deadline = System.currentTimeMillis() + 60000;
      while (first == null && System.currentTimeMillis() < deadline) {
        first = messenger.pollDocToProcess();
      }
      assertNotNull("first valid document should have been polled", first);
      assertEquals("doc1", first.getId());

      // the poison record at offset 1 now blocks the partition: the poll throws instead
      // of returning a document, and doc3 (offset 2) is never returned
      RecordDeserializationException poison = null;
      deadline = System.currentTimeMillis() + 60000;
      while (poison == null && System.currentTimeMillis() < deadline) {
        try {
          Document doc = messenger.pollDocToProcess();
          // a returned document here would mean the poison record was skipped -- that is
          // the desired FIXED behavior; today it never happens
          assertNull("no document should be returned past the poison record", doc);
        } catch (RecordDeserializationException e) {
          poison = e;
        }
      }
      assertNotNull("poll should have thrown RecordDeserializationException", poison);
      assertEquals("the poison record sits at offset 1", 1L, poison.offset());

      // retrying does not help: the consumer position was not advanced, so the very same
      // record poisons every subsequent poll -- the partition is permanently stuck
      try {
        messenger.pollDocToProcess();
        fail("expected the poll to keep throwing on the poison record");
      } catch (RecordDeserializationException e) {
        assertEquals(1L, e.offset());
      }
    } finally {
      messenger.close();
    }
  }

  /**
   * Demonstrates the defect end-to-end with a real Worker: the worker processes doc1, hits
   * the poison record, and its thread silently dies (Worker.run catches the exception from
   * the poll, logs "interrupted" at INFO, and returns). The committed offset for the group
   * is stuck at 1 while the topic holds 3 records, and doc3 is never sent for indexing.
   */
  @Test(timeout = 180000)
  public void testPoisonRecordKillsWorkerAndStrandsPartition() throws Exception {
    String pipelineName = "poison_worker";
    String groupId = "poison_worker_group";
    Config config = buildConfig(pipelineName, groupId);
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName, config);
    String destTopic = KafkaUtils.getDestTopicName(pipelineName);
    embeddedKafka.addTopics(new NewTopic(sourceTopic, 1, (short) 1));
    produceValidPoisonValid(sourceTopic);

    RecordingWorkerMessenger messenger =
        new RecordingWorkerMessenger(new KafkaWorkerMessenger(config, pipelineName));
    Worker worker = new Worker(config, messenger, "run1", pipelineName, pipelineName);
    WorkerThread workerThread = Worker.startThread(worker, "poison-worker");

    // the thread dies on its own once the poll hits the poison record; nobody calls stop()
    workerThread.join(120000);
    assertFalse("worker thread should have died after hitting the poison record",
        workerThread.isAlive());
    assertTrue("worker should have died on a RecordDeserializationException, but got: "
            + messenger.lastPollFailure,
        messenger.lastPollFailure instanceof RecordDeserializationException);

    // Worker.run's catch block returns without closing the messenger; clean up here
    messenger.close();

    // the group's committed offset is stuck at 1: doc1 was consumed and committed, but the
    // poison record (offset 1) and doc3 (offset 2) remain stranded on the partition forever
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    try (Admin kafkaAdminClient = Admin.create(adminProps)) {
      Map<TopicPartition, OffsetAndMetadata> committed =
          kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      TopicPartition sourcePartition = new TopicPartition(sourceTopic, 0);
      assertNotNull(committed.get(sourcePartition));
      assertEquals("committed offset should be stuck at 1, before the poison record",
          1L, committed.get(sourcePartition).offset());
    }

    // only doc1 ever made it to the destination topic; doc3 was never processed
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(embeddedKafka, "poison_worker_inspector", false);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    try (Consumer<String, String> inspector = cf.createConsumer()) {
      TopicPartition sourcePartition = new TopicPartition(sourceTopic, 0);
      TopicPartition destPartition = new TopicPartition(destTopic, 0);
      inspector.assign(List.of(sourcePartition, destPartition));

      // all three records are still present on the source topic...
      assertEquals(3L, inspector.endOffsets(List.of(sourcePartition)).get(sourcePartition).longValue());
      // ...but exactly one document (doc1) reached the destination topic
      assertEquals(1L, inspector.endOffsets(List.of(destPartition)).get(destPartition).longValue());

      inspector.seekToBeginning(List.of(destPartition));
      ConsumerRecords<String, String> destRecords = KafkaTestUtils.getRecords(inspector);
      assertEquals(1, destRecords.records(destPartition).size());
      Document indexedDoc = Document.createFromJson(destRecords.records(destPartition).get(0).value());
      assertEquals("doc1", indexedDoc.getId());
    }
  }

  /**
   * Delegating messenger that records the exception (if any) thrown by pollDocToProcess,
   * so the test can verify exactly what killed the worker thread.
   */
  private static class RecordingWorkerMessenger implements WorkerMessenger {

    private final WorkerMessenger delegate;
    private volatile Throwable lastPollFailure;

    RecordingWorkerMessenger(WorkerMessenger delegate) {
      this.delegate = delegate;
    }

    @Override
    public Document pollDocToProcess() throws Exception {
      try {
        return delegate.pollDocToProcess();
      } catch (Exception e) {
        lastPollFailure = e;
        throw e;
      }
    }

    @Override
    public void commitPendingDocOffsets() throws Exception {
      delegate.commitPendingDocOffsets();
    }

    @Override
    public void sendForIndexing(Document document) throws Exception {
      delegate.sendForIndexing(document);
    }

    @Override
    public void sendFailed(Document document) throws Exception {
      delegate.sendFailed(document);
    }

    @Override
    public void sendEvent(Document document, String message, Event.Type type) throws Exception {
      delegate.sendEvent(document, message, type);
    }

    @Override
    public void sendEvent(Event event) throws Exception {
      delegate.sendEvent(event);
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }
  }
}
