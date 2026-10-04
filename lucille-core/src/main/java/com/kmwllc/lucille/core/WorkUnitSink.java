package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Receives the work units a {@link PartitionableConnector} plans.
 */
public interface WorkUnitSink {

  /**
   * Hands one unit to the Coordinator for dispatch. May block while the Coordinator is applying back-pressure.
   *
   * @param unitKey identifies the unit within the connector. Must be the same every time the same source is planned,
   *                so that a resumed run can recognize units it already dispatched.
   * @param payload describes the slice of the source; passed back to {@link PartitionableConnector#executeUnit}.
   * @throws ConnectorException if the unit could not be dispatched, or the run has failed and planning should stop.
   */
  void emit(String unitKey, ObjectNode payload) throws ConnectorException;
}
