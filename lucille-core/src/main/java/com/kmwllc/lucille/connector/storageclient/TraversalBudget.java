package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.FailureClass;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Limits how much of a tree one traversal walks, and collects the directories it leaves unwalked.
 *
 * A StorageClient that is given a budget asks {@link #mayList()} before it lists each directory. Once the budget is
 * used up, it does not list the directory; it passes it to {@link #handBack(URI)} instead, and carries on to the
 * next directory it already knows of, handing that back too. Files that were already listed are still published.
 * Whoever started the traversal then arranges for the handed-back directories to be traversed separately.
 *
 * The first directory is always listed, whatever the limits, so that every traversal makes progress.
 *
 * A budget with no limits never hands anything back, and serves to count the directories listed.
 */
public class TraversalBudget {

  private final long maxDirectories;
  private final long deadlineMillis;
  private final BooleanSupplier cancelled;
  private final AtomicLong directoriesListed = new AtomicLong();
  private final AtomicLong refusedCalls = new AtomicLong();
  private final List<URI> handedBack = Collections.synchronizedList(new ArrayList<>());
  // set once the source has failed for good on some directory; nothing more is listed after that
  private volatile SourceError sourceError;

  /** A failure of the source that ended the traversal early. */
  public record SourceError(FailureClass failureClass, String cause) { }

  /**
   * @param maxDirectories the number of directories that may be listed, or null for no limit.
   * @param maxMillis how long the traversal may go on listing directories, or null for no limit.
   */
  public TraversalBudget(Integer maxDirectories, Long maxMillis) {
    this(maxDirectories, maxMillis, () -> false);
  }

  /**
   * @param cancelled says whether the traversal has been given up on. Once it has, no directory may be listed, not
   *                  even the first, so the traversal ends as soon as it next asks.
   */
  public TraversalBudget(Integer maxDirectories, Long maxMillis, BooleanSupplier cancelled) {
    this.maxDirectories = maxDirectories == null ? Long.MAX_VALUE : maxDirectories;
    this.deadlineMillis = maxMillis == null ? Long.MAX_VALUE : System.currentTimeMillis() + maxMillis;
    this.cancelled = cancelled;
  }

  /** A budget that only counts. */
  public static TraversalBudget unlimited() {
    return new TraversalBudget(null, null);
  }

  /**
   * Returns whether another directory may be listed, and counts it as listed if so.
   */
  public boolean mayList() {
    if (cancelled.getAsBoolean() || sourceError != null) {
      return false;
    }

    while (true) {
      long listed = directoriesListed.get();
      if (listed > 0 && (listed >= maxDirectories || System.currentTimeMillis() >= deadlineMillis)) {
        return false;
      }
      if (directoriesListed.compareAndSet(listed, listed + 1)) {
        return true;
      }
    }
  }

  /**
   * Records a directory that was not listed because the budget was used up.
   */
  public void handBack(URI directory) {
    handedBack.add(directory);
  }

  public long getDirectoriesListed() {
    return directoriesListed.get();
  }

  /** Records a call that the source refused or could not answer, and that was or will be tried again. */
  public void callRefused() {
    refusedCalls.incrementAndGet();
  }

  public long getRefusedCalls() {
    return refusedCalls.get();
  }

  /**
   * Records that the source failed for good on a directory, after whatever retries the policy allowed. From here on
   * the budget allows nothing more to be listed, so the rest of the traversal is handed back, to be tried again as
   * units of its own once the Coordinator sees fit. The directory itself should be handed back too.
   */
  public void sourceError(FailureClass failureClass, Throwable cause) {
    Throwable root = FailureClass.rootCause(cause);
    sourceError = new SourceError(failureClass, root.getClass().getSimpleName() + ": " + root.getMessage());
  }

  /** Takes back the count of a directory that mayList() allowed but that could not be listed after all. */
  public void listingFailed() {
    directoriesListed.decrementAndGet();
  }

  /** Returns the failure that ended the traversal early, or null if none did. */
  public SourceError getSourceError() {
    return sourceError;
  }

  /** Whether whoever started the traversal has given up on it. */
  public boolean isCancelled() {
    return cancelled.getAsBoolean();
  }

  /** Returns the directories that were left unwalked, in the order they were handed back. */
  public List<URI> getHandedBack() {
    synchronized (handedBack) {
      return List.copyOf(handedBack);
    }
  }
}
