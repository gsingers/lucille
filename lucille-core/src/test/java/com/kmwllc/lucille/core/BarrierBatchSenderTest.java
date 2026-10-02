package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

/**
 * Tests BarrierBatchSender directly: generation building (cap, declined batches leading the next generation), completion
 * order, and Error propagation. Batches are lists of documents whose IDs are their destination IDs; a document whose ID
 * starts with "dbq" is a delete-by-query. Each batch's send blocks on a latch keyed by its first document's ID.
 */
public class BarrierBatchSenderTest {

  private static final long TIMEOUT_MS = 10000;

  private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();
  // Batch names (first doc ID) in the order their sends started.
  private final List<String> started = Collections.synchronizedList(new ArrayList<>());
  private final List<String> completed = Collections.synchronizedList(new ArrayList<>());
  private final List<String> completionThreads = Collections.synchronizedList(new ArrayList<>());
  private volatile Error completionError;
  private volatile String failOn;
  private BarrierBatchSender sender;

  @After
  public void tearDown() {
    gates.values().forEach(CountDownLatch::countDown);
    if (sender != null) {
      sender.close();
    }
  }

  private BarrierBatchSender sender(int k) {
    sender = new BarrierBatchSender(k, "BarrierBatchSenderTest", this::send, this::complete,
        BarrierBatchSenderTest::ids, doc -> doc.getId().startsWith("dbq"));
    return sender;
  }

  private SendOutcome send(List<Document> batch) {
    String name = name(batch);
    started.add(name);
    CountDownLatch gate = gates.get(name);
    try {
      if (gate != null && !gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("gate " + name + " never released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return new SendOutcome(Set.of(), null, 0);
  }

  private void complete(List<Document> batch, SendOutcome outcome) {
    completed.add(name(batch));
    completionThreads.add(Thread.currentThread().getName());
    if (name(batch).equals(failOn)) {
      throw new StackOverflowError("boom");
    }
  }

  private static Set<String> ids(List<Document> batch) {
    return batch.stream().map(Document::getId).collect(Collectors.toCollection(HashSet::new));
  }

  private static String name(List<Document> batch) {
    return batch.get(0).getId();
  }

  private static List<Document> batch(String... ids) {
    List<Document> docs = new ArrayList<>();
    for (String id : ids) {
      docs.add(Document.create(id));
    }
    return docs;
  }

  private void gate(String... names) {
    for (String name : names) {
      gates.put(name, new CountDownLatch(1));
    }
  }

  private void release(String... names) {
    for (String name : names) {
      gates.get(name).countDown();
    }
  }

  // Sends a gated delete-by-query batch, which is a generation of its own, then the given batches, and releases the
  // warm-up once they are all queued, so the next generation is built from exactly them.
  private void sendBehindWarmup(List<List<Document>> batches) throws InterruptedException {
    gate("dbqWarm");
    sender.send(batch("dbqWarm"));
    awaitTrue(() -> started.contains("dbqWarm"));
    for (List<Document> b : batches) {
      sender.send(b);
    }
    awaitTrue(() -> sender.queued() == batches.size());
    release("dbqWarm");
  }

  @Test
  public void testGenerationIsCappedAtK() throws Exception {
    sender(2);
    gate("a", "b", "c");
    sendBehindWarmup(List.of(batch("a"), batch("b")));
    awaitTrue(() -> started.containsAll(List.of("a", "b")));
    // c fills the freed queue slot but cannot join a full generation.
    sender.send(batch("c"));
    Thread.sleep(100);
    // Sends within a generation start in any order.
    assertEquals(Set.of("dbqWarm", "a", "b"), Set.copyOf(started));

    release("a", "b", "c");
    sender.completeAll();
    assertEquals(List.of("dbqWarm", "a", "b", "c"), completed);
  }

  @Test
  public void testOverlappingBatchLeadsNextGeneration() throws Exception {
    sender(3);
    gate("a", "c", "a2");
    // a2 shares the ID "x" with a; c is disjoint from both.
    sendBehindWarmup(List.of(batch("a", "x"), batch("a2", "x"), batch("c")));
    awaitTrue(() -> started.contains("a"));
    Thread.sleep(100);
    // Peek, not skip: a2 is declined and stays at the head, so c (behind it) does not join either.
    assertEquals(List.of("dbqWarm", "a"), started);

    release("a");
    awaitTrue(() -> started.containsAll(List.of("a2", "c")));
    assertEquals("a2 leads the next generation, which c joins", Set.of("a2", "c"), Set.copyOf(started.subList(2, 4)));
    release("a2", "c");
    sender.completeAll();
    assertEquals(List.of("dbqWarm", "a", "a2", "c"), completed);
  }

  @Test
  public void testDeleteByQueryLeadsNextGenerationAlone() throws Exception {
    sender(3);
    gate("a", "dbq1", "b");
    sendBehindWarmup(List.of(batch("a"), batch("dbq1"), batch("b")));
    awaitTrue(() -> started.contains("a"));
    Thread.sleep(100);
    assertEquals(List.of("dbqWarm", "a"), started);

    release("a");
    awaitTrue(() -> started.contains("dbq1"));
    Thread.sleep(100);
    assertEquals("nothing joins a delete-by-query", List.of("dbqWarm", "a", "dbq1"), started);
    release("dbq1");
    awaitTrue(() -> started.contains("b"));
    release("b");
    sender.completeAll();
    assertEquals(List.of("dbqWarm", "a", "dbq1", "b"), completed);
  }

  @Test
  public void testCompletionFollowsDispatchOrderOnDispatcherThread() throws Exception {
    sender(3);
    gate("a", "b", "c");
    sendBehindWarmup(List.of(batch("a"), batch("b"), batch("c")));
    awaitTrue(() -> started.containsAll(List.of("a", "b", "c")));
    release("c");
    release("b");
    Thread.sleep(100);
    assertEquals("barrier: nothing completes before the whole generation is sent", List.of("dbqWarm"), completed);
    release("a");
    sender.completeAll();
    assertEquals(List.of("dbqWarm", "a", "b", "c"), completed);
    assertTrue(completionThreads.toString(),
        completionThreads.stream().allMatch(n -> n.contains("IndexerDispatch")));
  }

  @Test
  public void testErrorPropagatesToIndexerThread() throws Exception {
    sender(2);
    failOn = "a";
    sender.send(batch("a"));
    StackOverflowError error = assertThrows(StackOverflowError.class, () -> sender.completeAll());
    assertEquals("boom", error.getMessage());
    // and at every later call
    assertThrows(StackOverflowError.class, () -> sender.completeFinished());
    assertThrows(StackOverflowError.class, () -> sender.send(batch("b")));
  }

  @Test
  public void testErrorSurfacesWhileIndexerBlockedOnFullQueue() throws Exception {
    sender(1);
    failOn = "a";
    gate("a");
    sender.send(batch("a"));
    awaitTrue(() -> started.contains("a"));
    sender.send(batch("b")); // fills the queue
    Thread blocked = new Thread(() -> {
      try {
        sender.send(batch("c"));
      } catch (Error e) {
        completionError = e;
      }
    });
    blocked.start();
    release("a");
    blocked.join(TIMEOUT_MS);
    assertTrue(String.valueOf(completionError), completionError instanceof StackOverflowError);
  }

  @Test
  public void testInterruptDuringCompleteAllIsHeldAndRestored() throws Exception {
    sender(2);
    gate("a");
    sender.send(batch("a"));
    Thread.currentThread().interrupt();
    Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
      release("a");
    });
    releaser.start();
    sender.completeAll();
    assertTrue("interrupt restored", Thread.interrupted());
    assertEquals(List.of("a"), completed);
  }

  private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT_MS + " ms");
      }
      Thread.sleep(5);
    }
  }
}
