package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.WorkUnit;
import java.util.Map;

/**
 * API that the Coordinator of a distributed crawl uses to exchange messages with other components.
 *
 * In addition to what a Publisher needs, a Coordinator needs to 1) hand work units to Crawlers and 2) record its own
 * decisions as Events, alongside the Events it receives, so that the Events of a run are a complete log from which a
 * restarted Coordinator can rebuild its state.
 */
public interface CoordinatorMessenger extends PublisherMessenger {

  /**
   * Returns how many partitions the work topic has: the places a unit can be dispatched to. Each is read by at most
   * one Crawler thread at a time, in order, so the Coordinator uses them to decide how much work is in front of each
   * Crawler. Known once initialize() has been called.
   */
  int numWorkPartitions();

  /**
   * Makes a work unit available to Crawlers, on the given partition of the work topic. Returns once the unit has
   * been accepted.
   */
  void dispatchUnit(WorkUnit unit, int partition) throws Exception;

  /**
   * Records an Event in the run's Event log. Returns once the Event has been accepted. The Event will later be
   * returned by pollEvent(), in order with the Events sent by other components.
   */
  void sendEvent(Event event) throws Exception;

  /**
   * Records a UNIT_CREATED Event and then dispatches the unit it describes, in that order: the unit must not become
   * available to Crawlers before the Event has been accepted. May return before either has happened. If either
   * fails, a UNIT_FAILED Event for the unit is later returned by pollEvent(), so that the Coordinator can dispatch
   * the unit again as it would after a Crawler's failure; a failure that is not the unit's is thrown by a later call
   * to this method, to pollEvent() or to flush().
   */
  default void logAndDispatchUnit(Event unitCreated, WorkUnit unit, int partition) throws Exception {
    sendEvent(unitCreated);
    dispatchUnit(unit, partition);
  }

  /**
   * Returns what the units of an earlier run of the given pipeline cost to execute, by unit ID, as reported by the
   * Crawlers that executed them: the calls they made to their source, which for a file crawl is directories listed.
   * Empty if the run is not known. A Coordinator uses it to start the largest units first. May be called before
   * initialize().
   */
  default Map<String, Long> readUnitCosts(String runId, String pipelineName) throws Exception {
    return Map.of();
  }

  /**
   * Returns whether pollEvent() has returned every Event that was in the run's Event log when this messenger was
   * initialized. Always true for a messenger that was not asked to replay the log.
   */
  boolean replayComplete() throws Exception;
}
