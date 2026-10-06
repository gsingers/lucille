package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.RunControlTracker;
import com.kmwllc.lucille.core.WorkUnit;
import java.util.Map;
import java.util.TreeMap;
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

  private static final int DEFAULT_WORK_PARTITIONS = 16;
  private final int numWorkPartitions;

  private final LocalMessenger delegate;
  private final BlockingQueue<WorkUnit> workUnits = new LinkedBlockingQueue<>();
  private final RunControlTracker tracker;
  private final Map<String, Status> latestControl = new ConcurrentHashMap<>();

  public LocalCrawlMessenger(LocalMessenger delegate, long orphanTimeoutMillis) {
    this(delegate, orphanTimeoutMillis, DEFAULT_WORK_PARTITIONS);
  }

  public LocalCrawlMessenger(LocalMessenger delegate, long orphanTimeoutMillis, int numWorkPartitions) {
    this.numWorkPartitions = numWorkPartitions;
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
  public int numWorkPartitions() {
    // there is one queue, which every Crawler reads, so the number only decides how many units may be in flight
    return numWorkPartitions;
  }

  @Override
  public void dispatchUnit(WorkUnit unit, int partition) {
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

  // RunControl. Each Status in latestControl holds the time it was recorded in place of its age.

  @Override
  public Status latest(String runId) {
    return withAge(latestControl.get(runId));
  }

  @Override
  public Map<String, Status> list() {
    Map<String, Status> statuses = new TreeMap<>();
    latestControl.forEach((runId, status) -> statuses.put(runId, withAge(status)));
    return statuses;
  }

  private static Status withAge(Status status) {
    return status == null ? null : new Status(status.cancelled(), status.epoch(), status.configHash(),
        System.currentTimeMillis() - status.ageMillis(), status.reason());
  }

  @Override
  public void heartbeat(String runId, int epoch, String configHash) {
    // as on the control topic, the highest epoch counts, and a cancellation is final within its epoch
    latestControl.merge(runId, new Status(false, epoch, configHash, System.currentTimeMillis(), null), (current, update) ->
        update.epoch() > current.epoch() || (update.epoch() == current.epoch() && !current.cancelled()) ? update : current);
    tracker.onHeartbeat(runId, epoch, System.currentTimeMillis());
  }

  @Override
  public void cancel(String runId, int epoch, String configHash, String reason) {
    latestControl.merge(runId, new Status(true, epoch, configHash, System.currentTimeMillis(), reason),
        (current, update) -> update.epoch() >= current.epoch() ? update : current);
    tracker.onCancel(runId, epoch);
  }

  @Override
  public void close() {
  }
}
