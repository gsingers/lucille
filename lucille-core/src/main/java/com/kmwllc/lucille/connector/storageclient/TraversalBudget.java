package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.FailureClass;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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
  // told of each listing and each refusal as it happens, so that progress reports carry them before the unit ends
  private volatile Listener listener = Listener.NONE;
  // how many calls the unit may have in flight, by the run's latest word; null when nothing bounds them
  private volatile Supplier<Integer> maxConcurrency = () -> null;

  /** Hears of the traversal's listings, refusals, failure and hand-backs as they happen. */
  public interface Listener {
    Listener NONE = new Listener() { };

    default void listed() { }

    default void refused(FailureClass failureClass) { }

    default void requested() { }

    default void sourceError(SourceError error) { }

    default void handedBack(URI directory) { }
  }
  private final AtomicLong directoriesListed = new AtomicLong();
  private final AtomicLong refusedCalls = new AtomicLong();
  private final AtomicLong throttledCalls = new AtomicLong();
  private final AtomicLong requests = new AtomicLong();
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
        listener.listed();
        return true;
      }
    }
  }

  /**
   * Records a directory that was not listed because the budget was used up.
   */
  public void handBack(URI directory) {
    handedBack.add(directory);
    listener.handedBack(directory);
  }

  public long getDirectoriesListed() {
    return directoriesListed.get();
  }

  /**
   * Records a call that the source refused or could not answer, and that was or will be tried again. The class
   * matters to the run: a throttle slows it down at once, a call the source could not answer only when many do.
   */
  public void callRefused(FailureClass failureClass) {
    refusedCalls.incrementAndGet();
    if (failureClass == FailureClass.THROTTLED) {
      throttledCalls.incrementAndGet();
    }
    listener.refused(failureClass);
  }

  /** Records a refused call whose class is not known, as a throttle, since that is the safe thing to take it for. */
  public void callRefused() {
    callRefused(FailureClass.THROTTLED);
  }

  /** Returns how many of the refused calls the source throttled. */
  public long getThrottledCalls() {
    return throttledCalls.get();
  }

  /**
   * Records a request made to the source, whatever came of it: each page of a listing, each fetch, each failed
   * attempt. Refusals are set against these, so that a few among many are not taken for overload.
   */
  public void requestMade() {
    requests.incrementAndGet();
    listener.requested();
  }

  public long getRequests() {
    return requests.get();
  }

  public void setListener(Listener listener) {
    this.listener = listener == null ? Listener.NONE : listener;
  }

  /**
   * Sets where the traversal learns how many calls it may have in flight at once. A storage client that lists with
   * a pool of threads should cap the pool at {@link #getMaxConcurrency()} and look again between directories.
   */
  public void setMaxConcurrency(Supplier<Integer> maxConcurrency) {
    this.maxConcurrency = maxConcurrency == null ? () -> null : maxConcurrency;
  }

  /** Returns how many calls this traversal may have in flight at once, or null if nothing bounds them. */
  public Integer getMaxConcurrency() {
    return maxConcurrency.get();
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
    SourceError error = new SourceError(failureClass, root.getClass().getSimpleName() + ": " + root.getMessage());
    sourceError = error;
    listener.sourceError(error);
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
