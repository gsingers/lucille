package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.ScriptedPartitionedConnector;
import com.kmwllc.lucille.message.CoordinatorMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorPublisherTest {

  private static final String RUN_ID = "run1";

  private static final Config CONFIG = ConfigFactory.parseString("""
      connectors: [{
        name: "connector1", class: "com.kmwllc.lucille.connector.ScriptedPartitionedConnector",
        pipeline: "pipeline1", numUnits: 3, docsPerUnit: 2
      }]
      crawl { maxOutstandingUnits: 2, maxAttempts: 2 }
      """);

  /**
   * Records what the Publisher sends, in order, and returns the Events a test queues up. Events queued with
   * {@link #log} stand for a log left by an earlier Coordinator: replayComplete() is false until they are polled.
   */
  private static class ScriptedMessenger implements CoordinatorMessenger {

    final List<String> sent = new CopyOnWriteArrayList<>();
    final List<WorkUnit> dispatched = new CopyOnWriteArrayList<>();
    final ArrayDeque<Event> log = new ArrayDeque<>();

    @Override
    public void dispatchUnit(WorkUnit unit) {
      sent.add("DISPATCH " + unit.unitId());
      dispatched.add(unit);
    }

    @Override
    public void sendEvent(Event event) {
      sent.add(event.getType() + " " + event.getDocumentId());
    }

    @Override
    public boolean replayComplete() {
      return log.isEmpty();
    }

    @Override
    public Event pollEvent() {
      return log.poll();
    }

    @Override
    public void initialize(String runId, String pipelineName) {
    }

    @Override
    public String getRunId() {
      return RUN_ID;
    }

    @Override
    public void sendForProcessing(Document document) {
      sent.add("DOC " + document.getId());
    }

    @Override
    public void close() {
    }
  }

  private ScriptedMessenger messenger;
  private Connector connector;

  @Before
  public void setUp() throws Exception {
    messenger = new ScriptedMessenger();
    connector = new ScriptedPartitionedConnector(CONFIG.getConfigList("connectors").get(0));
  }

  private CoordinatorPublisher publisher(int epoch) throws Exception {
    return new CoordinatorPublisher(CONFIG, messenger, RUN_ID, connector, "test", epoch);
  }

  private static Event unitEvent(String unitKey, int attempt, int epoch, Event.Type type) {
    String message = CrawlConfig.newMessage().put("attempt", attempt).put("epoch", epoch).put("error", "boom").toString();
    return new Event("connector1/" + unitKey, RUN_ID, message, type);
  }

  private static Event docEvent(String docId, Event.Type type) {
    return new Event(docId, RUN_ID, null, type);
  }

  private static Event created(WorkUnit unit) {
    return new Event(unit.unitId(), RUN_ID, unit.toJson(), Event.Type.UNIT_CREATED);
  }

  private static WorkUnit unit(String unitKey, int attempt, int epoch) {
    return new WorkUnit(RUN_ID, "connector1", "pipeline1", "connector1/" + unitKey, attempt, epoch, "hash",
        WorkUnit.newPayload());
  }

  @Test
  public void testUnitIsLoggedBeforeItIsDispatched() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload().put("unit", 0));

    assertEquals(List.of("UNIT_CREATED connector1/u0", "DISPATCH connector1/u0"), messenger.sent);

    WorkUnit unit = messenger.dispatched.get(0);
    assertEquals(RUN_ID, unit.runId());
    assertEquals("connector1", unit.connectorName());
    assertEquals("pipeline1", unit.pipelineName());
    assertEquals(1, unit.attempt());
    assertEquals(1, unit.epoch());
    assertEquals(CrawlConfig.connectorConfigHash(CrawlConfig.connectorConfig(CONFIG, "connector1")), unit.configHash());
    assertEquals(0, unit.payload().get("unit").asInt());
    assertTrue(publisher.hasOutstandingWork());
  }

  @Test
  public void testUnitDone() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // the Coordinator reads its own UNIT_CREATED back from the log, which changes nothing
    publisher.handleEvent(created(messenger.dispatched.get(0)));
    assertEquals(1, publisher.numUnitsOutstanding());

    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    assertFalse(publisher.hasOutstandingWork());
    assertEquals(1, publisher.numUnitsDone());

    // a second report for the same unit, from a Crawler that executed a redelivered copy, changes nothing either
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    publisher.handleEvent(created(messenger.dispatched.get(0)));
    assertFalse(publisher.hasOutstandingWork());
    assertEquals(1, publisher.numUnitsDone());
  }

  @Test
  public void testFailedUnitIsDispatchedAgainUntilAttemptsRunOut() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_FAILED));
    assertNull(publisher.failureReason());
    assertEquals(2, messenger.dispatched.size());
    assertEquals(2, messenger.dispatched.get(1).attempt());
    assertTrue(publisher.hasOutstandingWork());

    // a report about the first attempt is out of date now that the second has been dispatched
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_FAILED));
    assertTrue(publisher.hasOutstandingWork());
    assertEquals(2, messenger.dispatched.size());

    // crawl.maxAttempts is 2
    publisher.handleEvent(unitEvent("u0", 2, 1, Event.Type.UNIT_FAILED));
    assertEquals(2, messenger.dispatched.size());
    assertNotNull(publisher.failureReason());
    assertTrue(publisher.failureReason().contains("connector1/u0"));
    assertTrue(publisher.failureReason().contains("boom"));
  }

  @Test
  public void testReportFromAnotherEpochIsIgnored() throws Exception {
    CoordinatorPublisher publisher = publisher(2);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    assertTrue(publisher.hasOutstandingWork());

    publisher.handleEvent(unitEvent("u0", 1, 2, Event.Type.UNIT_DONE));
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testEventsOfOtherRunsAndConnectorsAreIgnored() throws Exception {
    CoordinatorPublisher publisher = publisher(1);

    publisher.handleEvent(new Event("doc1", "anotherRun", null, Event.Type.CREATE));
    assertFalse(publisher.hasPending());

    WorkUnit foreign = new WorkUnit(RUN_ID, "connector2", "pipeline1", "connector2/u0", 1, 1, "hash", WorkUnit.newPayload());
    publisher.handleEvent(created(foreign));
    publisher.handleEvent(new Event("connector2", RUN_ID, null, Event.Type.PLANNING_DONE));
    publisher.handleEvent(new Event("connector2", RUN_ID, CoordinatorPublisher.HOOK_POST_EXECUTE, Event.Type.HOOK_DONE));

    assertFalse(publisher.hasOutstandingWork());
    assertFalse(publisher.isPlanningDone());
    assertFalse(publisher.isHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE));
  }

  @Test
  public void testMalformedEventsAreIgnored() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // none of these may throw: an exception here would end the run, and would end every attempt to resume it
    publisher.handleEvent(new Event("doc1", RUN_ID, null, null));
    publisher.handleEvent(new Event(null, RUN_ID, null, Event.Type.CREATE));
    publisher.handleEvent(new Event("connector1", RUN_ID, null, Event.Type.HOOK_DONE));
    publisher.handleEvent(new Event("connector1/u0", RUN_ID, null, Event.Type.UNIT_DONE));
    publisher.handleEvent(new Event("connector1/u0", RUN_ID, "not json", Event.Type.UNIT_DONE));
    publisher.handleEvent(new Event("connector1/u0", RUN_ID, "[1, 2]", Event.Type.UNIT_FAILED));
    publisher.handleEvent(new Event("connector1/u0", RUN_ID, "{}", Event.Type.UNIT_DONE));
    publisher.handleEvent(new Event("connector1/u9", RUN_ID, "not a unit", Event.Type.UNIT_CREATED));

    assertEquals(List.of("connector1/u0"), publisher.outstandingUnitIds());
    assertFalse(publisher.hasPending());
    assertNull(publisher.failureReason());
    assertFalse(publisher.isHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE));
  }

  @Test
  public void testDocumentsPublishedByCrawlersAreTracked() throws Exception {
    CoordinatorPublisher publisher = publisher(1);

    publisher.handleEvent(docEvent("doc1", Event.Type.CREATE));
    assertEquals(1, publisher.numPending());
    publisher.handleEvent(docEvent("doc1", Event.Type.FINISH));
    assertFalse(publisher.hasPending());

    // the Indexer can report a Document before the Crawler's CREATE for it arrives
    publisher.handleEvent(docEvent("doc2", Event.Type.FINISH));
    publisher.handleEvent(docEvent("doc2", Event.Type.CREATE));
    assertFalse(publisher.hasPending());

    // a unit that is executed twice publishes its Documents twice; each copy is created and finished
    publisher.handleEvent(docEvent("doc3", Event.Type.CREATE));
    publisher.handleEvent(docEvent("doc3", Event.Type.CREATE));
    publisher.handleEvent(docEvent("doc3", Event.Type.FINISH));
    assertTrue(publisher.hasPending());
    publisher.handleEvent(docEvent("doc3", Event.Type.FINISH));
    assertFalse(publisher.hasPending());

    // every copy that was indexed counts as a success, so repeated units inflate the count
    assertEquals(4, publisher.numSucceeded());
  }

  @Test
  public void testDispatchWaitsWhileTooManyUnitsAreOutstanding() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());

    // crawl.maxOutstandingUnits is 2, so the third unit has to wait
    Thread planner = new Thread(() -> {
      try {
        sink.emit("u2", WorkUnit.newPayload());
      } catch (ConnectorException e) {
        throw new RuntimeException(e);
      }
    });
    planner.start();
    planner.join(300);
    assertTrue(planner.isAlive());
    assertEquals(2, messenger.dispatched.size());

    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    planner.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(planner.isAlive());
    assertEquals(3, messenger.dispatched.size());
  }

  @Test
  public void testPlanningStopsOnceTheConnectorHasFailed() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_FAILED));
    publisher.handleEvent(unitEvent("u0", 2, 1, Event.Type.UNIT_FAILED));

    assertThrows(ConnectorException.class, () -> sink.emit("u1", WorkUnit.newPayload()));
  }

  @Test
  public void testRecoverRebuildsStateWithoutActingOnIt() throws Exception {
    // an earlier Coordinator, at epoch 1, got part way through planning and then stopped
    messenger.log.addAll(List.of(
        new Event("connector1", RUN_ID, CoordinatorPublisher.HOOK_PRE_EXECUTE, Event.Type.HOOK_DONE),
        created(unit("u0", 1, 1)),
        created(unit("u1", 1, 1)),
        docEvent("doc1", Event.Type.CREATE),
        docEvent("doc2", Event.Type.CREATE),
        unitEvent("u0", 1, 1, Event.Type.UNIT_DONE),
        docEvent("doc1", Event.Type.FINISH),
        unitEvent("u1", 1, 1, Event.Type.UNIT_FAILED),
        created(unit("u1", 2, 1)),
        unitEvent("u1", 2, 1, Event.Type.UNIT_FAILED)));

    CoordinatorPublisher publisher = publisher(2);
    publisher.recover();

    assertTrue(publisher.isHookDone(CoordinatorPublisher.HOOK_PRE_EXECUTE));
    assertFalse(publisher.isHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE));
    assertFalse(publisher.isPlanningDone());
    assertEquals(1, publisher.numUnitsDone());
    assertEquals(List.of("connector1/u1"), publisher.outstandingUnitIds());
    assertEquals(1, publisher.numPending());

    // u1 had used up its attempts, but that was the earlier Coordinator's failure to act on, not this one's
    assertNull(publisher.failureReason());
    assertTrue(messenger.sent.isEmpty());

    // the unit that was left outstanding is dispatched again, as a first attempt under the new epoch
    publisher.redispatchOutstandingUnits();
    assertEquals(List.of("UNIT_CREATED connector1/u1", "DISPATCH connector1/u1"), messenger.sent);
    assertEquals(2, messenger.dispatched.get(0).epoch());
    assertEquals(1, messenger.dispatched.get(0).attempt());

    // planning runs again, and only the unit that had not been reached is new
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());
    publisher.handleEvent(unitEvent("u1", 1, 2, Event.Type.UNIT_DONE));
    sink.emit("u2", WorkUnit.newPayload());

    List<String> dispatchedIds = new ArrayList<>();
    messenger.dispatched.forEach(unit -> dispatchedIds.add(unit.unitId()));
    assertEquals(List.of("connector1/u1", "connector1/u2"), dispatchedIds);
  }

  @Test
  public void testRecoverOfCompletedConnector() throws Exception {
    messenger.log.addAll(List.of(
        new Event("connector1", RUN_ID, CoordinatorPublisher.HOOK_PRE_EXECUTE, Event.Type.HOOK_DONE),
        created(unit("u0", 1, 1)),
        new Event("connector1", RUN_ID, null, Event.Type.PLANNING_DONE),
        docEvent("doc1", Event.Type.CREATE),
        unitEvent("u0", 1, 1, Event.Type.UNIT_DONE),
        docEvent("doc1", Event.Type.FINISH),
        new Event("connector1", RUN_ID, CoordinatorPublisher.HOOK_FINALIZE_RUN, Event.Type.HOOK_DONE),
        new Event("connector1", RUN_ID, CoordinatorPublisher.HOOK_POST_EXECUTE, Event.Type.HOOK_DONE)));

    CoordinatorPublisher publisher = publisher(2);
    publisher.recover();
    publisher.redispatchOutstandingUnits();

    assertTrue(publisher.isPlanningDone());
    assertTrue(publisher.isHookDone(CoordinatorPublisher.HOOK_POST_EXECUTE));
    assertFalse(publisher.hasOutstandingWork());
    assertFalse(publisher.hasPending());
    assertEquals(1, publisher.numSucceeded());
    assertTrue(messenger.sent.isEmpty());
  }

  @Test
  public void testLoggedDecisions() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.logHookDone(CoordinatorPublisher.HOOK_PRE_EXECUTE);
    publisher.logPlanningDone();

    assertTrue(publisher.isHookDone(CoordinatorPublisher.HOOK_PRE_EXECUTE));
    assertTrue(publisher.isPlanningDone());
    assertEquals(List.of("HOOK_DONE connector1", "PLANNING_DONE connector1"), messenger.sent);
  }
}
