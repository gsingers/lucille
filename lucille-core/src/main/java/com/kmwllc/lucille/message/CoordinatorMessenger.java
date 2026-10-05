package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.WorkUnit;

/**
 * API that the Coordinator of a distributed crawl uses to exchange messages with other components.
 *
 * In addition to what a Publisher needs, a Coordinator needs to 1) hand work units to Crawlers and 2) record its own
 * decisions as Events, alongside the Events it receives, so that the Events of a run are a complete log from which a
 * restarted Coordinator can rebuild its state.
 */
public interface CoordinatorMessenger extends PublisherMessenger {

  /**
   * Makes a work unit available to Crawlers. Returns once the unit has been accepted.
   */
  void dispatchUnit(WorkUnit unit) throws Exception;

  /**
   * Records an Event in the run's Event log. Returns once the Event has been accepted. The Event will later be
   * returned by pollEvent(), in order with the Events sent by other components.
   */
  void sendEvent(Event event) throws Exception;

  /**
   * Records a UNIT_CREATED Event and then dispatches the unit it describes, in that order: the unit must not become
   * available to Crawlers before the Event has been accepted. May return before either has happened. A failure
   * is reported by a later call to this method, to pollEvent() or to flush().
   */
  default void logAndDispatchUnit(Event unitCreated, WorkUnit unit) throws Exception {
    sendEvent(unitCreated);
    dispatchUnit(unit);
  }

  /**
   * Returns whether pollEvent() has returned every Event that was in the run's Event log when this messenger was
   * initialized. Always true for a messenger that was not asked to replay the log.
   */
  boolean replayComplete() throws Exception;
}
