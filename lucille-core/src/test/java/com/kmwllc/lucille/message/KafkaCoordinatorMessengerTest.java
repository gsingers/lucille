package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.CrawlConfig;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.WorkUnit;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests how KafkaCoordinatorMessenger attaches its consumer to a run's event topic. The topic has usually just been
 * created, and on a cluster of several brokers the broker that is asked about it may not know of it yet. A single
 * broker, such as the embedded one used by other tests, always knows, so a mock consumer stands in here.
 */
public class KafkaCoordinatorMessengerTest {

  private static final String TOPIC = "pipeline1_event_run1";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final List<PartitionInfo> ONE_PARTITION = List.of(new PartitionInfo(TOPIC, 0, null, null, null));

  private static final Config CONFIG = ConfigFactory.parseString("""
      kafka {
        bootstrapServers: "localhost:9092"
        consumerGroupId: "lucille_workers"
        maxPollIntervalSecs: 600
        maxRequestSize: 250000000
      }
      """);

  private Consumer<String, String> consumer;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    consumer = mock(Consumer.class);
    when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(PARTITION, 7L));
  }

  // A messenger that knows which topic it reads, as it would after the part of initialize() that needs a broker.
  private static KafkaCoordinatorMessenger messenger(boolean replay) throws Exception {
    KafkaCoordinatorMessenger messenger = new KafkaCoordinatorMessenger(CONFIG, replay);
    setField(messenger, "eventTopicName", TOPIC);
    return messenger;
  }

  private static void setField(KafkaCoordinatorMessenger messenger, String name, Object value) throws Exception {
    Field field = KafkaCoordinatorMessenger.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(messenger, value);
  }

  // A messenger whose sends complete only when the test says so, reading an event topic that has nothing in it.
  private KafkaCoordinatorMessenger messengerWith(MockProducer<String, String> producer) throws Exception {
    KafkaCoordinatorMessenger messenger = messenger(false);
    setField(messenger, "stringProducer", producer);
    setField(messenger, "documentProducer", new MockProducer<>(true, null, new StringSerializer(), new StringSerializer()));
    when(consumer.partitionsFor(TOPIC)).thenReturn(ONE_PARTITION);
    when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
    messenger.assignEventConsumer(consumer, 10_000);
    return messenger;
  }

  private static void waitForSends(MockProducer<String, String> producer, int count) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (producer.history().size() < count && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(count, producer.history().size());
  }

  private static Event unitFailed(Event event, WorkUnit unit) throws Exception {
    assertEquals(Event.Type.UNIT_FAILED, event.getType());
    assertEquals(unit.unitId(), event.getDocumentId());
    ObjectNode message = CrawlConfig.parseMessage(event.getMessage());
    assertEquals(unit.attempt(), message.get("attempt").asInt());
    assertEquals(unit.epoch(), message.get("epoch").asInt());
    return event;
  }

  @Test
  public void testUnitIsSentOnceItsEventIsAccepted() throws Exception {
    MockProducer<String, String> producer = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
    KafkaCoordinatorMessenger messenger = messengerWith(producer);
    WorkUnit unit = new WorkUnit("run1", "c", "pipeline1", "c/u0", 2, 3, "hash", WorkUnit.newPayload());

    messenger.logAndDispatchUnit(new Event(unit.unitId(), "run1", unit.toJson(), Event.Type.UNIT_CREATED), unit, 5);

    // only the Event has been handed to the producer
    waitForSends(producer, 1);
    assertEquals(TOPIC, producer.history().get(0).topic());
    Thread.sleep(100);
    assertEquals(1, producer.history().size());

    // once it is accepted, the unit follows, to the partition asked for
    producer.completeNext();
    waitForSends(producer, 2);
    ProducerRecord<String, String> work = producer.history().get(1);
    assertEquals("lucille_work", work.topic());
    assertEquals(Integer.valueOf(5), work.partition());
    assertEquals(unit.unitId(), work.key());
    assertEquals(unit, WorkUnit.fromJson(work.value()));

    producer.completeNext();
    assertNull(messenger.pollEvent());
    messenger.close();
  }

  @Test
  public void testUnitWhoseSendFailsIsReportedAsFailed() throws Exception {
    MockProducer<String, String> producer = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
    KafkaCoordinatorMessenger messenger = messengerWith(producer);
    WorkUnit unit = new WorkUnit("run1", "c", "pipeline1", "c/u0", 1, 1, "hash", WorkUnit.newPayload());
    Event created = new Event(unit.unitId(), "run1", unit.toJson(), Event.Type.UNIT_CREATED);

    // the Event cannot be written: the unit is not sent, and its failure is reported ahead of anything from the topic
    messenger.logAndDispatchUnit(created, unit, 0);
    waitForSends(producer, 1);
    producer.errorNext(new TimeoutException("broker away"));
    Thread.sleep(100);
    assertEquals(1, producer.history().size());
    Event report = unitFailed(messenger.pollEvent(), unit);
    assertTrue(report.getMessage().contains("UNIT_CREATED"));
    assertNull(messenger.pollEvent());

    // the Event is written but the unit cannot be sent: reported the same way, about the same dispatch
    WorkUnit again = unit.withAttempt(2);
    messenger.logAndDispatchUnit(new Event(again.unitId(), "run1", again.toJson(), Event.Type.UNIT_CREATED), again, 0);
    waitForSends(producer, 2);
    producer.completeNext();
    waitForSends(producer, 3);
    producer.errorNext(new TimeoutException("broker away again"));
    Event second = unitFailed(messenger.pollEvent(), again);
    assertTrue(second.getMessage().contains("work topic"));
    assertNull(messenger.pollEvent());

    // neither failure is the run's
    messenger.flush();
    messenger.close();
  }

  @Test
  public void testFlushCompletesEventsAndThenUnits() throws Exception {
    MockProducer<String, String> producer = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
    KafkaCoordinatorMessenger messenger = messengerWith(producer);

    for (int i = 0; i < 3; i++) {
      WorkUnit unit = new WorkUnit("run1", "c", "pipeline1", "c/u" + i, 1, 1, "hash", WorkUnit.newPayload());
      messenger.logAndDispatchUnit(new Event(unit.unitId(), "run1", unit.toJson(), Event.Type.UNIT_CREATED), unit, i);
    }

    // the units are handed to the producer only as their Events complete, which flush() has to see through
    messenger.flush();

    assertEquals(6, producer.history().size());
    assertEquals(3, producer.history().stream().filter(record -> record.topic().equals("lucille_work")).count());
    assertTrue(producer.history().subList(0, 3).stream().allMatch(record -> record.topic().equals(TOPIC)));
    assertNull(messenger.pollEvent());
    messenger.close();
  }

  @Test
  public void testWaitsForNewTopicToBecomeVisible() throws Exception {
    // the first two brokers asked have not heard of the topic; depending on the client, that is an empty list or null
    when(consumer.partitionsFor(TOPIC)).thenReturn(List.of()).thenReturn(null).thenReturn(ONE_PARTITION);

    messenger(false).assignEventConsumer(consumer, 10_000);

    verify(consumer, times(3)).partitionsFor(TOPIC);
    verify(consumer).assign(List.of(PARTITION));
    verify(consumer, never()).assign(List.of());
    // a run that is not being resumed reads only what is written from now on
    verify(consumer).seekToEnd(List.of(PARTITION));
    verify(consumer).position(PARTITION);
  }

  @Test
  public void testFailsIfTopicNeverBecomesVisible() throws Exception {
    when(consumer.partitionsFor(TOPIC)).thenReturn(List.of());
    KafkaCoordinatorMessenger messenger = messenger(false);

    // the failure belongs here, where it can say what is wrong, and not in the first poll for an Event
    Exception e = assertThrows(Exception.class, () -> messenger.assignEventConsumer(consumer, 300));
    assertTrue(e.getMessage(), e.getMessage().contains(TOPIC));
    verify(consumer, never()).assign(any());
  }

  @Test
  public void testReplayWaitsForTopicToo() throws Exception {
    when(consumer.partitionsFor(TOPIC)).thenReturn(List.of()).thenReturn(ONE_PARTITION);
    when(consumer.position(PARTITION)).thenReturn(0L);

    KafkaCoordinatorMessenger messenger = messenger(true);
    messenger.assignEventConsumer(consumer, 10_000);

    verify(consumer).seekToBeginning(List.of(PARTITION));
    // seven Events were in the topic and none has been read, so the replay is not over
    assertFalse(messenger.replayComplete());

    when(consumer.position(PARTITION)).thenReturn(7L);
    assertTrue(messenger.replayComplete());
  }

  @Test
  public void testReplayIsNotCompleteBeforeTheTopicIsAssigned() throws Exception {
    // a messenger asked to replay must not report that there was nothing to replay when it has not looked
    assertFalse(messenger(true).replayComplete());

    when(consumer.partitionsFor(TOPIC)).thenReturn(List.of());
    KafkaCoordinatorMessenger messenger = messenger(true);
    assertThrows(Exception.class, () -> messenger.assignEventConsumer(consumer, 300));
    assertFalse(messenger.replayComplete());

    // while one that was not asked to replay has nothing to wait for
    assertTrue(messenger(false).replayComplete());
  }

  @Test
  public void testPartitionOrderSpreadsConsecutiveUnitsOverConsumers() {
    // Kafka gives each consumer of a topic a run of neighbouring partitions. Units dealt to partitions 0, 1, 2, ...
    // would all go to the first consumer until its run was used up.
    for (int partitions : new int[] {1, 2, 3, 5, 8, 16, 24, 32}) {
      int[] order = CrawlConfig.interleavedPartitionOrder(partitions);

      // every partition is used, once
      assertEquals(partitions, order.length);
      assertEquals(partitions, Arrays.stream(order).distinct().count());
      assertTrue(Arrays.stream(order).allMatch(p -> p >= 0 && p < partitions));
    }

    // 32 partitions shared by 2 consumers, as 0-15 and 16-31: five units reach both consumers, not one
    int[] order = CrawlConfig.interleavedPartitionOrder(32);
    assertEquals(2, Arrays.stream(order).limit(2).map(p -> p / 16).distinct().count());
    assertEquals(List.of(3L, 2L), List.of(
        Arrays.stream(order).limit(5).filter(p -> p < 16).count(),
        Arrays.stream(order).limit(5).filter(p -> p >= 16).count()));

    // and however many consumers share the partitions evenly, the first units go one to each
    for (int consumers : new int[] {2, 4, 8, 16}) {
      int blockSize = 32 / consumers;
      assertEquals("consumers: " + consumers, consumers,
          Arrays.stream(order).limit(consumers).map(p -> p / blockSize).distinct().count());
    }

    // the same holds when the number of partitions is not a power of two
    int[] twentyFour = CrawlConfig.interleavedPartitionOrder(24);
    for (int consumers : new int[] {2, 3, 4}) {
      int blockSize = 24 / consumers;
      assertEquals("consumers: " + consumers, consumers,
          Arrays.stream(twentyFour).limit(2L * consumers).map(p -> p / blockSize).distinct().count());
    }
  }

  @Test
  public void testEmptyTopicHasNothingToReplay() throws Exception {
    when(consumer.partitionsFor(TOPIC)).thenReturn(ONE_PARTITION);
    when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(PARTITION, 0L));
    when(consumer.position(PARTITION)).thenReturn(0L);

    KafkaCoordinatorMessenger messenger = messenger(true);
    messenger.assignEventConsumer(consumer, 10_000);

    assertEquals(true, messenger.replayComplete());
  }
}
