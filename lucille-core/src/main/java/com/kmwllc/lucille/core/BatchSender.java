package com.kmwllc.lucille.core;

import java.util.List;

/**
 * How an {@link Indexer} sends its flushed batches to the destination and completes them (sends their events and calls
 * {@link com.kmwllc.lucille.message.IndexerMessenger#batchComplete(List)}). The Indexer holds one, chosen by
 * indexer.maxConcurrentBatches, and calls it the same way whatever the implementation:
 * {@link SynchronousBatchSender} when it is 1, {@link BarrierBatchSender} otherwise. ({@link ConcurrentBatchSender}, the
 * sliding window, is kept for comparison but unused.)
 *
 * <p> Contract, for every implementation:
 * <ul>
 *   <li>All methods are called on the indexer thread. Batches are completed on the indexer thread
 *   (SynchronousBatchSender, ConcurrentBatchSender) or on a single dispatcher thread (BarrierBatchSender).</li>
 *   <li>Every batch passed to {@link #send(List)} is completed exactly once, in the order the batches were sent.</li>
 *   <li>After {@link #completeAll()} returns, every batch sent so far has been completed.</li>
 *   <li>No method throws because a send failed: failures are reported through the batch's completion.</li>
 * </ul>
 */
interface BatchSender {

  /** Sends a non-empty batch. May block until the batch, or batches sent before it, have been sent and completed. */
  void send(List<Document> batch);

  /** Completes batches whose sends have already finished, without waiting for any others. */
  void completeFinished();

  /** Waits for every batch sent so far and completes it. */
  void completeAll();

  /** Releases any resources. Called once, after the last call to any other method. */
  void close();
}
