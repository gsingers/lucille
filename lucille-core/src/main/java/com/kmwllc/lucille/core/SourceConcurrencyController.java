package com.kmwllc.lucille.core;

import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides how many calls a distributed crawl may have in flight against its source at once, and adjusts it to what
 * the source will take: additive increase, multiplicative decrease, as TCP does.
 *
 * The Coordinator calls {@link #adjust(Interval)} once per heartbeat with what Crawlers reported since the last
 * time: how many calls they made, how many the source throttled, and how many it could not answer. A throttle is the
 * source asking for less, so any throttle halves the figure. A source that could not answer (a 5xx, a connection
 * failure) halves it only when that happens to more than a set share of the calls: a source that fails now and then
 * under any load is not overloaded, and each such failure has cost its own retry already. The figure is not halved
 * again within the hold time, so that reports still arriving from the burst that caused the first halving do not
 * halve it twice; an interval that does not halve it adds a step. The figure starts below the maximum and climbs, so
 * a source that scales on demand is given time to.
 *
 * Nothing here is shared with Crawlers but the result: the figure divided among the units in flight, which goes out
 * in the heartbeat as each unit's allowance.
 */
public class SourceConcurrencyController {

  private static final Logger log = LoggerFactory.getLogger(SourceConcurrencyController.class);

  /** The share of calls that may go unanswered before that is taken as overload, by default: one in a thousand. */
  public static final double DEFAULT_UNAVAILABLE_RATE = 0.001;

  /**
   * What Crawlers reported over one interval.
   *
   * @param calls the calls made to the source.
   * @param throttled the calls the source throttled (HTTP 429, or the like).
   * @param unavailable the calls the source could not answer (a 5xx, a connection failure).
   */
  public record Interval(long calls, long throttled, long unavailable) { }

  private final int max;
  private final int step;
  private final int floor;
  private final long holdMillis;
  private final double unavailableRate;
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
   * @param unavailableRate the share of an interval's calls the source may fail to answer without that cutting the
   *     figure. 0 cuts on any.
   */
  public SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis, double unavailableRate) {
    this(max, initial, step, floor, holdMillis, unavailableRate, System::currentTimeMillis);
  }

  public SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis) {
    this(max, initial, step, floor, holdMillis, DEFAULT_UNAVAILABLE_RATE);
  }

  SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis, LongSupplier clock) {
    this(max, initial, step, floor, holdMillis, DEFAULT_UNAVAILABLE_RATE, clock);
  }

  SourceConcurrencyController(int max, int initial, int step, int floor, long holdMillis, double unavailableRate,
      LongSupplier clock) {
    if (max < 1 || step < 1 || floor < 1 || floor > max) {
      throw new IllegalArgumentException("Source concurrency needs max >= floor >= 1 and step >= 1.");
    }
    if (unavailableRate < 0 || unavailableRate >= 1) {
      throw new IllegalArgumentException("The unavailable rate must be at least 0 and less than 1.");
    }
    this.max = max;
    this.step = step;
    this.floor = floor;
    this.holdMillis = holdMillis;
    this.unavailableRate = unavailableRate;
    this.clock = clock;
    this.current = Math.max(floor, Math.min(max, initial));
  }

  /**
   * Takes account of an interval in which the source throttled the given number of calls, and nothing else is
   * known, and returns the figure for the next interval.
   */
  public int adjust(long throttledCalls) {
    return adjust(new Interval(0, throttledCalls, 0));
  }

  /**
   * Takes account of an interval's reports and returns the figure for the next interval.
   */
  public synchronized int adjust(Interval interval) {
    long now = clock.getAsLong();
    int before = current;
    String what = describe(interval);

    if (isOverload(interval)) {
      if (current == floor) {
        log.info("The source {}; source concurrency is already at its floor of {}.", what, floor);
      } else if (!everDecreased || now - lastDecreaseMillis >= holdMillis) {
        current = Math.max(floor, current / 2);
        everDecreased = true;
        lastDecreaseMillis = now;
        log.warn("The source {}; source concurrency cut from {} to {}.", what, before, current);
      } else {
        log.info("The source {}; source concurrency held at {} after the last cut.", what, current);
      }
    } else if (current < max) {
      current = Math.min(max, current + step);
      log.info("The source {}; source concurrency raised from {} to {}.", what, before, current);
    }

    return current;
  }

  private boolean isOverload(Interval interval) {
    return interval.throttled() > 0
        || (interval.unavailable() > 0 && interval.unavailable() > unavailableRate * interval.calls());
  }

  private static String describe(Interval interval) {
    if (interval.throttled() == 0 && interval.unavailable() == 0) {
      return "refused no calls";
    }
    return "throttled " + interval.throttled() + " and could not answer " + interval.unavailable() + " of "
        + interval.calls() + " calls";
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
