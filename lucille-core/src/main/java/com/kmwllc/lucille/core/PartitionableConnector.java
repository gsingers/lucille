package com.kmwllc.lucille.core;

/**
 * A Connector that can split its source into work units, so that a distributed crawl can execute those units in
 * parallel on separate Crawler processes.
 *
 * During a distributed crawl:
 *      preExecute() is called once, on the Coordinator
 *      prepareRun() is called once, on the Coordinator
 *      plan() is called once, on the Coordinator
 *      executeUnit() is called once per unit, on whichever Crawler receives the unit; a unit may be executed more
 *          than once if a Crawler fails or the run is resumed
 *      finalizeRun() is called once, on the Coordinator, after every unit and every published Document is complete
 *      postExecute() is called once, on the Coordinator
 *
 * execute() is not called. A Coordinator that is restarted with the same run ID may call a lifecycle method again
 * if it stopped while that method was running, so these methods should be safe to repeat.
 */
public interface PartitionableConnector extends Connector {

  /**
   * Returns whether this instance is configured to be partitioned. When false, the connector is run as a single
   * unit that calls execute(), exactly as a Connector that does not implement this interface.
   */
  default boolean isPartitioningEnabled() {
    return true;
  }

  /**
   * Performs any logic that has to happen exactly once before the first unit is executed. Unlike plan(), it is not
   * called again by a Coordinator that resumes the run after it has returned.
   */
  default void prepareRun(String runId) throws ConnectorException {
  }

  /**
   * Splits the source into units, passing each to the sink. A Coordinator that resumes a run whose planning was
   * interrupted calls this again, and skips the units that were already dispatched, so the same source must always
   * be split into the same units.
   */
  void plan(String runId, WorkUnitSink sink) throws ConnectorException;

  /**
   * Publishes the Documents belonging to one unit. Executing the same unit again must publish Documents with the
   * same IDs, so that a repeated unit overwrites what the earlier execution indexed instead of duplicating it.
   */
  void executeUnit(WorkUnit unit, Publisher publisher) throws ConnectorException;

  /**
   * Performs any logic that needs a view of the whole run, which no single unit has. Documents may be published.
   */
  default void finalizeRun(String runId, Publisher publisher) throws ConnectorException {
  }
}
