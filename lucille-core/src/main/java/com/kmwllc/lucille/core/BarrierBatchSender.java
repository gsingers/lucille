package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import com.kmwllc.lucille.util.ThreadNameUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Sends an Indexer's batches in generations of up to K, where K is indexer.maxConcurrentBatches (greater than 1). A
 * comparison implementation of the barrier (generational) design; see {@link ConcurrentBatchSender} for the sliding window.
 *
 * <p> The model is a barrier: {@link #send(List)} puts the batch on a ready queue of capacity K, blocking while it is
 * full. A dispatcher thread takes the head of the queue as a generation, adds following batches while they may run
 * alongside it, sends the generation on a pool of K threads, waits for all of its sends, then completes its batches in
 * order, on the dispatcher thread. The indexer thread only polls and enqueues, so IndexerMessenger completion calls
 * (sendEvent(s), batchComplete) run on the dispatcher thread, concurrently with the indexer thread's polls.
 *
 * <p> Guarantees:
 * <ul>
 *   <li>At most K batches are in flight (and at most K more wait in the queue).</li>
 *   <li>A generation is a run of consecutive queued batches, so batches are completed in the order they were sent.</li>
 *   <li>Batches in one generation have disjoint destination IDs (a document's own ID, or its idOverrideField value, and
 *   its children's document IDs, recursively), and generations never overlap in time.</li>
 *   <li>A batch containing a delete-by-query document forms a generation of its own.</li>
 *   <li>A batch that cannot join a generation stays at the head of the queue and leads the next one.</li>
 *   <li>An interrupt of the indexer thread neither drops a batch being enqueued nor cuts short {@link #completeAll()}:
 *   it is held while the thread waits, then restored. Completion never sees it, as it runs on the dispatcher.</li>
 *   <li>A Throwable thrown while completing a batch (an Error from completeBatch) stops the dispatcher and is rethrown on
 *   the indexer thread at its next send, completeFinished, or completeAll. Batches not yet completed are abandoned.</li>
 * </ul>
 */
class BarrierBatchSender implements BatchSender {

  private static final Logger log = LoggerFactory.getLogger(BarrierBatchSender.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;
  private static final long WAIT_SLICE_MS = 50;
  private static final AtomicInteger INSTANCES = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final Function<List<Document>, SendOutcome> send;
  private final BiConsumer<List<Document>, SendOutcome> complete;
  private final Function<List<Document>, Set<String>> destinationIds;
  private final Predicate<Document> isBarrier;
  private final ExecutorService pool;
  private final Thread dispatcher;
  private final BlockingQueue<Ready> ready;

  // Guards completed and the condition below. sent is touched only by the indexer thread.
  private final Object lock = new Object();
  private long sent;
  private long completed;
  // Set by the dispatcher when completion throws; the dispatcher then exits.
  private volatile Throwable failure;
  private volatile boolean closing;

  private record Ready(List<Document> docs, Set<String> ids, boolean barrier, Map<String, String> mdc) {}

  /**
   * @param send sends a batch; runs on the pool and must not throw.
   * @param complete completes a sent batch; runs on the dispatcher thread.
   * @param destinationIds the IDs a batch writes at the destination; batches sharing one are never in one generation.
   * @param isBarrier whether a document (a delete-by-query) requires its batch to be a generation of its own.
   */
  BarrierBatchSender(int maxConcurrentBatches, String runId, Function<List<Document>, SendOutcome> send,
      BiConsumer<List<Document>, SendOutcome> complete, Function<List<Document>, Set<String>> destinationIds,
      Predicate<Document> isBarrier) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.send = send;
    this.complete = complete;
    this.destinationIds = destinationIds;
    this.isBarrier = isBarrier;
    this.ready = new ArrayBlockingQueue<>(maxConcurrentBatches);
    int instance = INSTANCES.incrementAndGet();
    AtomicInteger count = new AtomicInteger();
    this.pool = Executors.newFixedThreadPool(maxConcurrentBatches, task -> {
      Thread thread = new Thread(task,
          ThreadNameUtils.createName("IndexerSend-" + instance + "-" + count.incrementAndGet(), runId));
      thread.setDaemon(true);
      return thread;
    });
    // Started on the first send, so an Indexer that is never run holds no thread.
    this.dispatcher = new Thread(this::dispatchLoop, ThreadNameUtils.createName("IndexerDispatch-" + instance, runId));
    this.dispatcher.setDaemon(true);
  }

  /** Puts the batch on the ready queue, blocking while it is full. */
  @Override
  public void send(List<Document> batch) {
    rethrowFailure();
    if (dispatcher.getState() == Thread.State.NEW) {
      dispatcher.start();
    }
    Ready entry = new Ready(batch, destinationIds.apply(batch), batch.stream().anyMatch(isBarrier),
        MDC.getCopyOfContextMap());
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        try {
          if (ready.offer(entry, WAIT_SLICE_MS, TimeUnit.MILLISECONDS)) {
            sent++;
            return;
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
        // The dispatcher may have died with the queue full; don't wait for it forever.
        rethrowFailure();
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Completion happens on the dispatcher; this only surfaces its failure. */
  @Override
  public void completeFinished() {
    rethrowFailure();
  }

  /** Waits until every batch sent so far has been completed. */
  @Override
  public void completeAll() {
    boolean interrupted = Thread.interrupted();
    try {
      synchronized (lock) {
        while (completed < sent) {
          rethrowFailure();
          try {
            lock.wait(WAIT_SLICE_MS);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
      rethrowFailure();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Stops the dispatcher and the pool. Batches still outstanding (only on the Error path) are abandoned. */
  @Override
  public void close() {
    closing = true;
    long outstanding;
    synchronized (lock) {
      outstanding = sent - completed;
    }
    if (outstanding > 0) {
      log.warn("Shutting down indexer with {} batches not completed.", outstanding);
      pool.shutdownNow();
    } else {
      pool.shutdown();
    }
    dispatcher.interrupt();
    try {
      dispatcher.join(SHUTDOWN_TIMEOUT_MS);
      if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        log.warn("Indexer send pool did not terminate within {} ms.", SHUTDOWN_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // for tests: the number of batches waiting in the ready queue
  int queued() {
    return ready.size();
  }

  private void rethrowFailure() {
    Throwable t = failure;
    if (t instanceof Error error) {
      throw error;
    } else if (t instanceof RuntimeException e) {
      throw e;
    } else if (t != null) {
      throw new IllegalStateException("Batch completion failed", t);
    }
  }

  private void dispatchLoop() {
    try {
      while (!closing) {
        List<Ready> generation = nextGeneration();
        List<Future<SendOutcome>> outcomes = new ArrayList<>(generation.size());
        for (Ready batch : generation) {
          outcomes.add(pool.submit(() -> sendWithMdc(batch)));
        }
        // The barrier: every send in the generation finishes before any batch is completed or another is sent.
        List<SendOutcome> results = new ArrayList<>(generation.size());
        for (Future<SendOutcome> outcome : outcomes) {
          results.add(await(outcome));
        }
        for (int i = 0; i < generation.size(); i++) {
          completeWithMdc(generation.get(i), results.get(i));
          synchronized (lock) {
            completed++;
            lock.notifyAll();
          }
        }
      }
    } catch (InterruptedException e) {
      // close(): idle, or abandoning a generation on the Error path
    } catch (Throwable t) {
      failure = t;
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  // Takes the head of the queue, then adds following batches while each is not a delete-by-query and its destination IDs
  // are disjoint from the generation's. A declined batch is left at the head (peek, not take) to lead the next generation.
  private List<Ready> nextGeneration() throws InterruptedException {
    Ready first = ready.take();
    List<Ready> generation = new ArrayList<>(maxConcurrentBatches);
    generation.add(first);
    if (first.barrier()) {
      return generation;
    }
    Set<String> union = new HashSet<>(first.ids());
    while (generation.size() < maxConcurrentBatches) {
      Ready next = ready.peek();
      if (next == null || next.barrier() || overlaps(union, next.ids())) {
        break;
      }
      ready.poll(); // the dispatcher is the only consumer, so this removes next
      generation.add(next);
      union.addAll(next.ids());
    }
    return generation;
  }

  private static boolean overlaps(Set<String> union, Set<String> ids) {
    for (String id : ids) {
      if (union.contains(id)) {
        return true;
      }
    }
    return false;
  }

  private SendOutcome sendWithMdc(Ready batch) {
    if (batch.mdc() != null) {
      MDC.setContextMap(batch.mdc());
    }
    try {
      return send.apply(batch.docs());
    } finally {
      MDC.clear();
    }
  }

  private void completeWithMdc(Ready batch, SendOutcome outcome) {
    if (batch.mdc() != null) {
      MDC.setContextMap(batch.mdc());
    }
    try {
      complete.accept(batch.docs(), outcome);
    } finally {
      MDC.clear();
    }
  }

  // Interrupted only by close(), which abandons the generation.
  private static SendOutcome await(Future<SendOutcome> future) throws InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      // send() captures its own Throwables, so this only happens if the task wrapper itself failed.
      return new SendOutcome(null, e.getCause(), 0);
    }
  }
}
