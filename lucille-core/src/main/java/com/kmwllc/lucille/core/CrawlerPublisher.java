package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.RunControlTracker.Decision;
import com.kmwllc.lucille.message.CrawlerMessenger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Publisher a connector is given while it executes one work unit on a Crawler.
 *
 * Unlike PublisherImpl, it keeps no record of what it published and receives no Events. The Coordinator of the run
 * does the tracking, from the CREATE Event that the messenger sends for each Document. So this Publisher never
 * blocks on the number of pending documents either; the Coordinator applies back-pressure by holding back units.
 *
 * Each call to publish() also checks that the unit is still worth executing. Once it is not, publish() throws and
 * {@link #getAbortReason()} says why. A connector may catch what publish() throws and carry on, so the Crawler must
 * check getAbortReason() after the unit returns.
 */
class CrawlerPublisher implements Publisher {

  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private final CrawlerMessenger messenger;
  private final WorkUnit unit;
  private final String pipelineName;
  private final boolean isCollapsing;

  private final AtomicLong numReceived = new AtomicLong(0);
  private final AtomicLong numPublished = new AtomicLong(0);
  private Document previousDoc = null;

  // null while the unit should keep executing; otherwise, why it should stop
  private volatile Decision abortReason = null;
  private volatile boolean unitLost = false;

  CrawlerPublisher(CrawlerMessenger messenger, WorkUnit unit, String pipelineName, boolean isCollapsing) {
    this.messenger = messenger;
    this.unit = unit;
    this.pipelineName = pipelineName;
    this.isCollapsing = isCollapsing;
  }

  @Override
  public void publish(Document document) throws Exception {
    checkStillActive();

    if (!isCollapsing) {
      sendForProcessing(document);
    } else if (previousDoc == null) {
      previousDoc = document;
    } else if (previousDoc.getId().equals(document.getId())) {
      previousDoc.setOrAddAll(document);
    } else {
      sendForProcessing(previousDoc);
      previousDoc = document;
    }

    numReceived.incrementAndGet();
  }

  private void sendForProcessing(Document document) throws Exception {
    document.initializeRunId(unit.runId());
    docLogger.info("Publishing document {}.", document.getId());
    messenger.sendForProcessing(document, pipelineName);
    numPublished.incrementAndGet();
  }

  private void checkStillActive() throws ConnectorException {
    if (messenger.isUnitLost()) {
      unitLost = true;
      throw new ConnectorException("Work unit " + unit.unitId() + " was reassigned to another Crawler.");
    }

    Decision decision = messenger.getRunControlTracker().decide(unit.runId(), unit.epoch());
    if (decision != Decision.RUN) {
      abortReason = decision;
      throw new ConnectorException("Work unit " + unit.unitId() + " abandoned: run is " + decision + ".");
    }
  }

  /**
   * Returns why the unit should no longer be executed, or null if it should.
   */
  Decision getAbortReason() {
    return abortReason;
  }

  /**
   * Returns whether the unit was taken away from this Crawler while it was executing.
   */
  boolean isUnitLost() {
    return unitLost || messenger.isUnitLost();
  }

  @Override
  public void flush() throws Exception {
    if (previousDoc != null) {
      sendForProcessing(previousDoc);
    }
    previousDoc = null;
    messenger.flush(pipelineName);
  }

  @Override
  public long numPublished() {
    return numPublished.get();
  }

  @Override
  public long numReceived() {
    return numReceived.get();
  }

  // The remaining methods concern tracking and Events, which are the Coordinator's job.

  @Override
  public long numPending() {
    return 0;
  }

  @Override
  public long numCreated() {
    return 0;
  }

  @Override
  public long numSucceeded() {
    return 0;
  }

  @Override
  public long numFailed() {
    return 0;
  }

  @Override
  public long numDropped() {
    return 0;
  }

  @Override
  public boolean hasPending() {
    return false;
  }

  @Override
  public void handleEvent(Event event) {
    throw new UnsupportedOperationException("A Crawler does not receive Events.");
  }

  @Override
  public PublisherResult waitForCompletion(ConnectorThread thread, int timeout) {
    throw new UnsupportedOperationException("A Crawler does not wait for completion.");
  }

  @Override
  public void preClose() {
  }

  @Override
  public void close() {
  }

  @Override
  public void pause() {
    throw new UnsupportedOperationException("A Crawler's Publisher cannot be paused.");
  }

  @Override
  public void resume() {
    throw new UnsupportedOperationException("A Crawler's Publisher cannot be paused.");
  }
}
