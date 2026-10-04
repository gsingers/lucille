package com.kmwllc.lucille.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * A Crawler's view of which runs are alive, built from the heartbeats and cancellations that Coordinators send.
 * A Crawler consults it before starting a unit and while executing one, so that it does not keep working for a run
 * whose Coordinator has gone away, was cancelled, or has been restarted under a newer epoch.
 */
public class RunControlTracker {

  public enum Decision {
    /** The run is alive and the unit belongs to its current epoch. */
    RUN,
    /** A newer Coordinator epoch has taken over the run; it has dispatched the unit again. */
    STALE,
    /** The run was cancelled or has finished. */
    CANCELLED,
    /** The run's Coordinator has not been heard from within the orphan timeout. */
    ORPHANED,
    /** No heartbeat for the unit's epoch has been seen yet. */
    UNKNOWN
  }

  private record RunState(int epoch, long lastSeenMillis, boolean cancelled) {
  }

  private final ConcurrentHashMap<String, RunState> runs = new ConcurrentHashMap<>();
  private final long orphanTimeoutMillis;
  private final LongSupplier clock;

  public RunControlTracker(long orphanTimeoutMillis) {
    this(orphanTimeoutMillis, System::currentTimeMillis);
  }

  RunControlTracker(long orphanTimeoutMillis, LongSupplier clock) {
    this.orphanTimeoutMillis = orphanTimeoutMillis;
    this.clock = clock;
  }

  /**
   * Records a heartbeat. A heartbeat from an epoch older than the newest one seen is ignored, so a superseded
   * Coordinator that is still running cannot keep its units alive.
   */
  public void onHeartbeat(String runId, int epoch, long seenAtMillis) {
    runs.merge(runId, new RunState(epoch, seenAtMillis, false), (current, update) -> {
      if (update.epoch() < current.epoch() || (update.epoch() == current.epoch() && current.cancelled())) {
        return current;
      }
      return update;
    });
  }

  public void onCancel(String runId, int epoch) {
    runs.merge(runId, new RunState(epoch, clock.getAsLong(), true),
        (current, update) -> update.epoch() < current.epoch() ? current : update);
  }

  public Decision decide(String runId, int unitEpoch) {
    RunState state = runs.get(runId);

    if (state == null || unitEpoch > state.epoch()) {
      return Decision.UNKNOWN;
    }
    if (unitEpoch < state.epoch()) {
      return Decision.STALE;
    }
    if (state.cancelled()) {
      return Decision.CANCELLED;
    }
    if (clock.getAsLong() - state.lastSeenMillis() > orphanTimeoutMillis) {
      return Decision.ORPHANED;
    }
    return Decision.RUN;
  }

  /**
   * Like {@link #decide}, but gives a heartbeat that has not arrived yet up to the given time to do so. A unit can
   * reach a Crawler before the Crawler has read its run's first heartbeat. Returns UNKNOWN if none arrives.
   */
  public Decision awaitDecision(String runId, int unitEpoch, long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    Decision decision = decide(runId, unitEpoch);

    while (decision == Decision.UNKNOWN && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
      decision = decide(runId, unitEpoch);
    }

    return decision;
  }

  /** Forgets runs that have been cancelled or silent for longer than the given age. */
  public void prune(long maxAgeMillis) {
    long cutoff = clock.getAsLong() - maxAgeMillis;
    runs.values().removeIf(state -> state.lastSeenMillis() < cutoff);
  }
}
