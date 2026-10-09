package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.RunControlTracker;
import com.kmwllc.lucille.core.WorkUnit;

/**
 * API that a Crawler uses to exchange messages with other components.
 *
 * A Crawler needs to 1) receive work units, 2) submit the Documents of a unit for processing, 3) send Events telling
 * the Coordinator about those Documents and about the unit itself, and 4) confirm a unit once it is finished with it.
 * A Crawler serves units from any run and any pipeline, so the pipeline is named on each call.
 */
public interface CrawlerMessenger {

  /**
   * Retrieves a work unit, or returns null if none became available within a short timeout. The unit is held by this
   * messenger until ackWorkUnit() or releaseWorkUnit() is called; no other unit will be returned before then. If the Crawler stops
   * without acknowledging the unit, the unit is delivered again, to this or another Crawler.
   */
  WorkUnit pollWorkUnit() throws Exception;

  /**
   * Submits a Document for processing by the given pipeline. May return before the Document has been accepted;
   * once it has, a CREATE Event for it is sent to the Coordinator. A Document that is not accepted causes a later
   * call to flush() to fail.
   */
  void sendForProcessing(Document document, String pipelineName) throws Exception;

  /**
   * Sends an Event about the held unit to the Coordinator of its run. Returns once the Event has been accepted.
   */
  void sendEvent(Event event, String pipelineName) throws Exception;

  /**
   * Returns once every Document submitted so far, and the CREATE Event for each, has been accepted.
   *
   * @throws Exception if any of them could not be delivered.
   */
  void flush(String pipelineName) throws Exception;

  /**
   * Confirms that the Crawler is finished with the held unit, so that it will not be delivered again.
   */
  void ackWorkUnit() throws Exception;

  /**
   * Gives up the held unit without confirming it, so that it will be delivered again. For a Crawler that could not
   * tell the Coordinator what became of the unit.
   */
  void releaseWorkUnit();

  /**
   * Returns whether the held unit has been taken away from this Crawler and will be delivered to another, in
   * which case this Crawler should stop executing it.
   */
  boolean isUnitLost();

  /**
   * Returns the tracker that reflects the heartbeats and cancellations sent by Coordinators.
   */
  RunControlTracker getRunControlTracker();

  void close();
}
