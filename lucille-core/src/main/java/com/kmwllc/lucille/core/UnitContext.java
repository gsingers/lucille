package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * What a {@link PartitionableConnector} is given, besides a Publisher, while it executes one work unit.
 */
public interface UnitContext {

  /**
   * Hands a part of this unit back, to be executed as a unit of its own, by this or another Crawler. For a unit that
   * turns out to be larger than one unit should be. The connector must not also do the part itself.
   *
   * The Crawler sends what has been handed back with its progress reports, every heartbeat, and the Coordinator
   * dispatches it at once (unless crawl.dispatchHandBacksEarly is false), so a part should be handed back as soon as
   * the connector knows it will not do it. If the unit then fails, the parts already sent stand, and the unit is
   * executed again from the start: those parts may be crawled twice, so Document IDs must not depend on which unit
   * found a Document. Parts handed back after {@link #recordSourceError} are held until the unit completes.
   *
   * @param unitKey identifies the part within the connector, as the key given to {@link WorkUnitSink#emit} does.
   *                Handing back a part under a key that already names a unit of the run has no effect, so the same
   *                part of the source must always be given the same key.
   * @param payload describes the part; passed to {@link PartitionableConnector#executeUnit} when it is executed.
   */
  void handBack(String unitKey, ObjectNode payload);

  /**
   * Records calls that executing this unit made to the source, such as directory listings. Reported to the
   * Coordinator with the unit's result, as a measure of what the unit cost that the number of Documents is not.
   */
  void addSourceCalls(long calls);

  /**
   * Records calls to the source that it refused or could not answer, whether or not a retry then succeeded. Reported
   * with the unit's result; the Coordinator reads them as a sign that the source is overloaded.
   */
  default void addRefusedCalls(long calls) {
  }

  /**
   * As {@link #addRefusedCalls(long)}, saying how they were refused. A throttle ({@link FailureClass#THROTTLED})
   * slows the run at once; a call the source could not answer ({@link FailureClass#SOURCE_UNAVAILABLE}) only when
   * more than crawl.sourceUnavailableRate of the calls are. Refusals counted without a class are taken as throttles.
   */
  default void addRefusedCalls(long calls, FailureClass failureClass) {
    addRefusedCalls(calls);
  }

  /**
   * Records requests made to the source, whatever came of them: each page of a listing, each fetch, each failed
   * attempt. The Coordinator sets refusals against these when deciding whether the source is overloaded. A connector
   * that does not count them is judged against its source calls instead.
   */
  default void addRequests(long requests) {
  }

  /**
   * Records that the source failed for good on part of this unit, which the connector has handed back rather than
   * executed, so that the unit can complete. The Coordinator delays the handed-back parts if the failure says the
   * source is overloaded.
   *
   * Call this before handing back what the failure left undone, so that it is held for the unit's completion
   * instead of being dispatched at once into the same failure.
   *
   * @param cause the innermost exception's class and message.
   */
  default void recordSourceError(FailureClass failureClass, String cause) {
  }

  /**
   * Returns how many calls to the source this unit may have in flight at once, by the run's latest heartbeat, or
   * null if nothing bounds them. The Coordinator divides the run's allowance (crawl.maxSourceConcurrency) among the
   * units in flight and lowers it when the source refuses calls, so a connector that lists with a pool of threads
   * should cap the pool at this and look again now and then.
   */
  default Integer maxSourceConcurrency() {
    return null;
  }

  /**
   * Returns whether this execution of the unit has been given up on: it ran past crawl.maxUnitSecs, its run was
   * cancelled or lost its Coordinator, or the unit was passed to another Crawler. Nothing the unit publishes or
   * hands back from then on counts. A connector whose units can be long should look now and then, and return.
   */
  default boolean isCancelled() {
    return false;
  }
}
