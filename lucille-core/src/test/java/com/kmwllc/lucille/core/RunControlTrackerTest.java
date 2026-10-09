package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.kmwllc.lucille.core.RunControlTracker.Decision;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class RunControlTrackerTest {

  @Test
  public void testUnitConcurrencyFollowsTheLatestHeartbeat() {
    RunControlTracker tracker = new RunControlTracker(60_000);
    assertNull(tracker.unitConcurrency("run1"));

    tracker.onHeartbeat("run1", 1, 1000, 100);
    assertEquals(Integer.valueOf(100), tracker.unitConcurrency("run1"));
    tracker.onHeartbeat("run1", 1, 2000, 50);
    assertEquals(Integer.valueOf(50), tracker.unitConcurrency("run1"));
    // a heartbeat that says nothing about it means nothing bounds the source
    tracker.onHeartbeat("run1", 1, 3000);
    assertNull(tracker.unitConcurrency("run1"));
    // a stale Coordinator's heartbeat does not set it
    tracker.onHeartbeat("run1", 2, 4000, 10);
    tracker.onHeartbeat("run1", 1, 5000, 999);
    assertEquals(Integer.valueOf(10), tracker.unitConcurrency("run1"));
  }

  private final AtomicLong clock = new AtomicLong(1_000_000);
  private final RunControlTracker tracker = new RunControlTracker(5000, clock::get);

  @Test
  public void testUnknownUntilHeartbeat() {
    assertEquals(Decision.UNKNOWN, tracker.decide("run1", 1));

    tracker.onHeartbeat("run1", 1, clock.get());
    assertEquals(Decision.RUN, tracker.decide("run1", 1));
    // a heartbeat for one run says nothing about another
    assertEquals(Decision.UNKNOWN, tracker.decide("run2", 1));
  }

  @Test
  public void testOrphanedWhenHeartbeatsStop() {
    tracker.onHeartbeat("run1", 1, clock.get());

    clock.addAndGet(5000);
    assertEquals(Decision.RUN, tracker.decide("run1", 1));

    clock.addAndGet(1);
    assertEquals(Decision.ORPHANED, tracker.decide("run1", 1));

    // the run is alive again as soon as a heartbeat arrives
    tracker.onHeartbeat("run1", 1, clock.get());
    assertEquals(Decision.RUN, tracker.decide("run1", 1));
  }

  @Test
  public void testNewerEpochMakesOlderUnitsStale() {
    tracker.onHeartbeat("run1", 1, clock.get());
    tracker.onHeartbeat("run1", 2, clock.get());

    assertEquals(Decision.STALE, tracker.decide("run1", 1));
    assertEquals(Decision.RUN, tracker.decide("run1", 2));
    // a unit can arrive before the heartbeat announcing its epoch
    assertEquals(Decision.UNKNOWN, tracker.decide("run1", 3));
  }

  @Test
  public void testHeartbeatFromSupersededEpochIsIgnored() {
    tracker.onHeartbeat("run1", 2, clock.get());
    clock.addAndGet(6000);

    // the first Coordinator is still running and still sending heartbeats; they must not revive epoch 2
    tracker.onHeartbeat("run1", 1, clock.get());
    assertEquals(Decision.ORPHANED, tracker.decide("run1", 2));
    assertEquals(Decision.STALE, tracker.decide("run1", 1));
  }

  @Test
  public void testCancel() {
    tracker.onHeartbeat("run1", 1, clock.get());
    tracker.onCancel("run1", 1);
    assertEquals(Decision.CANCELLED, tracker.decide("run1", 1));

    // a heartbeat that was already on its way when the run was cancelled does not undo the cancellation
    tracker.onHeartbeat("run1", 1, clock.get());
    assertEquals(Decision.CANCELLED, tracker.decide("run1", 1));

    // resuming the run under a new epoch does
    tracker.onHeartbeat("run1", 2, clock.get());
    assertEquals(Decision.RUN, tracker.decide("run1", 2));
    assertEquals(Decision.STALE, tracker.decide("run1", 1));
  }

  @Test
  public void testCancelFromSupersededEpochIsIgnored() {
    tracker.onHeartbeat("run1", 2, clock.get());
    tracker.onCancel("run1", 1);
    assertEquals(Decision.RUN, tracker.decide("run1", 2));
  }

  @Test
  public void testAwaitDecisionGivesUpOnUnknownRun() throws Exception {
    assertEquals(Decision.UNKNOWN, tracker.awaitDecision("run1", 1, 150));

    tracker.onHeartbeat("run1", 1, clock.get());
    assertEquals(Decision.RUN, tracker.awaitDecision("run1", 1, 150));
  }

  @Test
  public void testPrune() {
    tracker.onHeartbeat("old", 1, clock.get());
    clock.addAndGet(10_000);
    tracker.onHeartbeat("recent", 1, clock.get());

    tracker.prune(5000);
    assertEquals(Decision.UNKNOWN, tracker.decide("old", 1));
    assertEquals(Decision.RUN, tracker.decide("recent", 1));
  }
}
