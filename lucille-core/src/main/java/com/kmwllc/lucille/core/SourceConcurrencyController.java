package com.kmwllc.lucille.core;

import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides how many calls a distributed crawl may have in flight against its source at once, and adjusts it to what
 * the source will take: additive increase, multiplicative decrease, as TCP does.
 *
 * The Coordinator calls {@link #adjust(long)} once per heartbeat with the number of calls Crawlers reported as
 * refused since the last time. Any refusal halves the figure, though not again within the hold time, so that
 * reports still arriving from the burst that caused the first halving do not halve it twice; a clean interval adds
 * a step. The figure starts below the maximum and climbs, so a source that scales on demand is given time to.
 *
 * Nothing here is shared with Crawlers but the result: the figure divided among the units in flight, which goes out
 * in the heartbeat as each unit's allowance.
 */
public class SourceConcurrencyController {

  private static final Logger log = LoggerFactory.getLogger(SourceConcurrencyController.class);

  private final int max;
  private final int step;
  private final int floor;
  private final long holdMillis;
  private final LongSupplier clock;
  private volatile int current;
  private boolean everDecreased = false;
  private long lastDecreaseMillis;

  /**
   * @param max the most calls the run may have in flight.
   * @param initial where to start; the figure climbs from here while reports are clean.
   * @param step how much a clean interval adds.
   * @param floor the least the figure is cut to: one call for each unit in flight, or the units could not run.
   * @param holdMillis how long after a halving before the figure may be halved again.
   */
  public SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis) {
    this(max, initial, step, floor, holdMillis, System::currentTimeMillis);
  }

  SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis, LongSupplier clock) {
    if (max < 1 || step < 1 || floor < 1 || floor > max) {
      throw new IllegalArgumentException("Source concurrency needs max >= floor >= 1 and step >= 1.");
    }
    this.max = max;
    this.step = step;
    this.floor = floor;
    this.holdMillis = holdMillis;
    this.clock = clock;
    this.current = Math.max(floor, Math.min(max, initial));
  }

  /**
   * Takes account of an interval's reports and returns the figure for the next interval.
   *
   * @param refusedCalls how many calls the source refused during the interval.
   */
  public synchronized int adjust(long refusedCalls) {
    long now = clock.getAsLong();
    int before = current;

    if (refusedCalls > 0) {
      if (current == floor) {
        log.info("The source refused {} calls; source concurrency is already at its floor of {}.", refusedCalls, floor);
      } else if (!everDecreased || now - lastDecreaseMillis >= holdMillis) {
        current = Math.max(floor, current / 2);
        everDecreased = true;
        lastDecreaseMillis = now;
        log.warn("The source refused {} calls; source concurrency cut from {} to {}.", refusedCalls, before, current);
      } else {
        log.info("The source refused {} calls; source concurrency held at {} after the last cut.", refusedCalls, current);
      }
    } else if (current < max) {
      current = Math.min(max, current + step);
      log.info("No calls refused; source concurrency raised from {} to {}.", before, current);
    }

    return current;
  }

  /** The figure for the run as a whole. */
  public int current() {
    return current;
  }

  public int max() {
    return max;
  }

  /**
   * How many units may be in flight for the figure: as many as there are partitions to run them on, but never more
   * than there are calls, since a unit makes at least one call at a time.
   */
  public int unitsInFlight(int partitions) {
    return Math.max(1, Math.min(partitions, current));
  }

  /** The figure divided among the units in flight, at least one call each. */
  public int perUnit(int partitions) {
    return Math.max(1, current / unitsInFlight(partitions));
  }
}
