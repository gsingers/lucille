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
   * Nothing is handed back unless the unit then completes: if it fails, or its Crawler stops, the unit is executed
   * again from the start and may hand back different parts.
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
   * Returns whether this execution of the unit has been given up on: it ran past crawl.maxUnitSecs, its run was
   * cancelled or lost its Coordinator, or the unit was passed to another Crawler. Nothing the unit publishes or
   * hands back from then on counts. A connector whose units can be long should look now and then, and return.
   */
  default boolean isCancelled() {
    return false;
  }
}
