package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.RunControlTracker;
import com.kmwllc.lucille.core.WorkUnit;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Carries the message traffic of a distributed crawl in memory, so that a Coordinator and Crawlers running as threads
 * of one JVM can be exercised without Kafka. One instance is shared by all of them. Documents and Events are
 * passed to a LocalMessenger, which Workers and an Indexer in the same JVM can share.
 *
 * Work units that are polled but never acknowledged are not delivered again, and the Events of a run are not kept,
 * so this messenger cannot be used to replay a run.
 */
public class LocalCrawlMessenger implements CoordinatorMessenger, CrawlerMessenger, RunControl {

  private final LocalMessenger delegate;
  private final BlockingQueue<WorkUnit> workUnits = new LinkedBlockingQueue<>();
  private final RunControlTracker tracker;
  private final Map<String, Status> latestControl = new ConcurrentHashMap<>();

  public LocalCrawlMessenger(LocalMessenger delegate, long orphanTimeoutMillis) {
    this.delegate = delegate;
    this.tracker = new RunControlTracker(orphanTimeoutMillis);
  }

  // PublisherMessenger

  @Override
  public void initialize(String runId, String pipelineName) {
    // Unlike a LocalMessenger, one instance serves every connector of a run, so there is nothing to bind here.
  }

  @Override
  public String getRunId() {
    return delegate.getRunId();
  }

  @Override
  public void sendForProcessing(Document document) throws Exception {
    delegate.sendForProcessing(document);
  }

  @Override
  public Event pollEvent() throws Exception {
    return delegate.pollEvent();
  }

  // CoordinatorMessenger

  @Override
  public void dispatchUnit(WorkUnit unit) {
    workUnits.add(unit);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    delegate.sendEvent(event);
  }

  @Override
  public boolean replayComplete() {
    return true;
  }

  // CrawlerMessenger

  @Override
  public WorkUnit pollWorkUnit() throws Exception {
    return workUnits.poll(LocalMessenger.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sendForProcessing(Document document, String pipelineName) throws Exception {
    // the Event is built first because the Document may be changed by a Worker as soon as it is sent
    Event create = new Event(document, null, Event.Type.CREATE);
    delegate.sendForProcessing(document);
    delegate.sendEvent(create);
  }

  @Override
  public void sendEvent(Event event, String pipelineName) throws Exception {
    delegate.sendEvent(event);
  }

  @Override
  public void flush(String pipelineName) {
  }

  @Override
  public void ackWorkUnit() {
  }

  @Override
  public void releaseWorkUnit() {
  }

  @Override
  public boolean isUnitLost() {
    return false;
  }

  @Override
  public RunControlTracker getRunControlTracker() {
    return tracker;
  }

  // RunControl

  @Override
  public Status latest(String runId) {
    return latestControl.get(runId);
  }

  @Override
  public void heartbeat(String runId, int epoch, String configHash) {
    latestControl.put(runId, new Status(false, epoch, configHash, 0));
    tracker.onHeartbeat(runId, epoch, System.currentTimeMillis());
  }

  @Override
  public void cancel(String runId, int epoch, String configHash, String reason) {
    latestControl.put(runId, new Status(true, epoch, configHash, 0));
    tracker.onCancel(runId, epoch);
  }

  @Override
  public void close() {
  }
}
