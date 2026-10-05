package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
    Field eventTopicName = KafkaCoordinatorMessenger.class.getDeclaredField("eventTopicName");
    eventTopicName.setAccessible(true);
    eventTopicName.set(messenger, TOPIC);
    return messenger;
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
      int[] order = KafkaCoordinatorMessenger.interleavedPartitionOrder(partitions);

      // every partition is used, once
      assertEquals(partitions, order.length);
      assertEquals(partitions, Arrays.stream(order).distinct().count());
      assertTrue(Arrays.stream(order).allMatch(p -> p >= 0 && p < partitions));
    }

    // 32 partitions shared by 2 consumers, as 0-15 and 16-31: five units reach both consumers, not one
    int[] order = KafkaCoordinatorMessenger.interleavedPartitionOrder(32);
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
    int[] twentyFour = KafkaCoordinatorMessenger.interleavedPartitionOrder(24);
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
