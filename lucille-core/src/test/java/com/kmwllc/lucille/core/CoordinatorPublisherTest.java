package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.connector.ScriptedPartitionedConnector;
import com.kmwllc.lucille.message.CoordinatorMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

    final List<Integer> partitions = new CopyOnWriteArrayList<>();

    @Override
    public int numWorkPartitions() {
      return 2;
    }

    @Override
    public void dispatchUnit(WorkUnit unit, int partition) {
      sent.add("DISPATCH " + unit.unitId());
      dispatched.add(unit);
      partitions.add(partition);
    }

    @Override
    public Map<String, Long> readUnitCosts(String runId, String pipelineName) {
      return Map.of();
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
    return new CoordinatorPublisher(CONFIG, messenger, RUN_ID, connector, "test", epoch, Map.of());
  }

  // What a Crawler sends for an execution of a unit that handed parts back: the parts, then the completion.
  private static Event childrenEvent(String unitKey, int attempt, int epoch, String execution, String... partKeys) {
    ObjectNode message = CrawlConfig.newMessage().put("attempt", attempt).put("epoch", epoch).put("execution", execution);
    ArrayNode children = message.putArray("children");
    for (String partKey : partKeys) {
      children.addObject().put("key", partKey).putObject("payload").put("key", partKey);
    }
    return new Event("connector1/" + unitKey, RUN_ID, message.toString(), Event.Type.UNIT_CHILDREN);
  }

  private static Event doneEvent(String unitKey, int attempt, int epoch, String execution, int numChildren) {
    String message = CrawlConfig.newMessage().put("attempt", attempt).put("epoch", epoch).put("execution", execution)
        .put("numChildren", numChildren).toString();
    return new Event("connector1/" + unitKey, RUN_ID, message, Event.Type.UNIT_DONE);
  }

  private List<String> dispatchedKeys() {
    List<String> keys = new ArrayList<>();
    messenger.dispatched.forEach(unit -> keys.add(unit.unitId().substring("connector1/".length())));
    return keys;
  }

  @Test
  public void testHandedBackPartsBecomeUnits() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // the parts arrive ahead of the completion, and nothing is done with them until it does
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1"));
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p2"));
    assertEquals(List.of("u0"), dispatchedKeys());

    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 2));
    assertEquals(List.of("u0", "p1", "p2"), dispatchedKeys());
    assertEquals(1, publisher.numUnitsDone());
    assertEquals(2, publisher.numUnitsOutstanding());

    // each part is logged before it is dispatched, like a planned unit, and carries what the Crawler described
    assertTrue(messenger.sent.indexOf("UNIT_CREATED connector1/p1") < messenger.sent.indexOf("DISPATCH connector1/p1"));
    WorkUnit part = messenger.dispatched.get(1);
    assertEquals("p1", part.payload().get("key").asText());
    assertEquals(1, part.attempt());
    assertEquals(1, part.epoch());
    assertEquals(messenger.dispatched.get(0).configHash(), part.configHash());
    assertEquals("pipeline1", part.pipelineName());

    // the connector is not complete until the parts are done, and a part may hand back parts of its own
    publisher.handleEvent(childrenEvent("p1", 1, 1, "y", "p3"));
    publisher.handleEvent(doneEvent("p1", 1, 1, "y", 1));
    publisher.handleEvent(doneEvent("p2", 1, 1, "z", 0));
    assertTrue(publisher.hasOutstandingWork());
    publisher.handleEvent(doneEvent("p3", 1, 1, "w", 0));
    assertFalse(publisher.hasOutstandingWork());
    assertEquals(4, publisher.numUnitsDone());
  }

  @Test
  public void testHandedBackPartsWaitForRoom() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());

    // crawl.maxOutstandingUnits is 2. With u1 still outstanding there is room for one of the three parts.
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1", "p2", "p3"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 3));
    assertEquals(List.of("u0", "u1", "p1"), dispatchedKeys());

    // the two that are waiting are units all the same: they keep the connector from completing
    assertEquals(4, publisher.numUnitsOutstanding());
    assertTrue(publisher.outstandingUnitIds().containsAll(List.of("connector1/p2", "connector1/p3")));
    // and the planner coming to one of them is not a reason to create it again
    sink.emit("p3", WorkUnit.newPayload());
    assertEquals(3, messenger.dispatched.size());

    // they are dispatched as units complete, in the order they were handed back
    publisher.handleEvent(doneEvent("u1", 1, 1, "y", 0));
    assertEquals(List.of("u0", "u1", "p1", "p2"), dispatchedKeys());
    publisher.handleEvent(doneEvent("p1", 1, 1, "y", 0));
    assertEquals(List.of("u0", "u1", "p1", "p2", "p3"), dispatchedKeys());
    publisher.handleEvent(doneEvent("p2", 1, 1, "y", 0));
    publisher.handleEvent(doneEvent("p3", 1, 1, "y", 0));
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testHandedBackPartsWaitWhileTooManyDocumentsArePending() throws Exception {
    Config config = ConfigFactory.parseString("publisher.maxPendingDocs: 1").withFallback(CONFIG);
    CoordinatorPublisher publisher = new CoordinatorPublisher(config, messenger, RUN_ID, connector, "test", 1, Map.of());
    publisher.getSink().emit("u0", WorkUnit.newPayload());
    publisher.handleEvent(docEvent("doc1", Event.Type.CREATE));

    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 1));
    assertEquals(List.of("u0"), dispatchedKeys());
    assertTrue(publisher.hasOutstandingWork());

    // no unit completes to make room, so it is the wait loop that notices the Document has finished
    publisher.handleEvent(docEvent("doc1", Event.Type.FINISH));
    publisher.onWaitIteration();
    assertEquals(List.of("u0", "p1"), dispatchedKeys());
  }

  @Test
  public void testOnlyTheExecutionThatCompletedHandsBack() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // The unit was delivered twice and two Crawlers executed it, cutting it differently. Their reports interleave.
    publisher.handleEvent(childrenEvent("u0", 1, 1, "first", "a"));
    publisher.handleEvent(childrenEvent("u0", 1, 1, "second", "b", "c"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "second", 2));
    assertEquals(List.of("u0", "b", "c"), dispatchedKeys());

    // the other execution's completion is a report on a unit that is no longer outstanding
    publisher.handleEvent(doneEvent("u0", 1, 1, "first", 1));
    assertEquals(List.of("u0", "b", "c"), dispatchedKeys());
    assertEquals(1, publisher.numUnitsDone());
  }

  @Test
  public void testUnitWhoseHandedBackPartsDidNotAllArriveIsDispatchedAgain() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // the unit says it handed back two parts, and one arrived
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 2));

    // accepting it would leave the other part uncrawled, so the whole unit is done again
    assertEquals(List.of("u0", "u0"), dispatchedKeys());
    assertEquals(2, messenger.dispatched.get(1).attempt());
    assertEquals(0, publisher.numUnitsDone());

    publisher.handleEvent(childrenEvent("u0", 2, 1, "y", "p1", "p2"));
    publisher.handleEvent(doneEvent("u0", 2, 1, "y", 2));
    assertEquals(List.of("u0", "u0", "p1", "p2"), dispatchedKeys());
  }

  @Test
  public void testFailedExecutionHandsNothingBack() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1"));
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_FAILED));
    assertEquals(List.of("u0", "u0"), dispatchedKeys());

    // the second attempt walks all of the unit itself, and that is the account of it that stands
    publisher.handleEvent(doneEvent("u0", 2, 1, "y", 0));
    assertEquals(List.of("u0", "u0"), dispatchedKeys());
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testPartThatIsAlreadyAUnitIsNotCreatedAgain() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());

    // u1 is outstanding: handing it back creates nothing
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "u1", "p1"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 2));
    assertEquals(List.of("u0", "u1", "p1"), dispatchedKeys());

    // p1 is done: handing it back creates nothing either, or two units could hand each other back forever
    publisher.handleEvent(doneEvent("p1", 1, 1, "y", 0));
    publisher.handleEvent(childrenEvent("u1", 1, 1, "z", "p1", "u0"));
    publisher.handleEvent(doneEvent("u1", 1, 1, "z", 2));
    assertEquals(List.of("u0", "u1", "p1"), dispatchedKeys());
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testMalformedHandedBackPartsAreIgnored() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    String attemptAndEpoch = "\"attempt\": 1, \"epoch\": 1, \"execution\": \"x\"";
    publisher.handleEvent(new Event("connector1/u0", RUN_ID, "{" + attemptAndEpoch + ", \"children\": \"p1\"}",
        Event.Type.UNIT_CHILDREN));
    publisher.handleEvent(new Event("connector1/u0", RUN_ID,
        "{" + attemptAndEpoch + ", \"children\": [{\"key\": 5}, {\"key\": \"p2\"}, \"p3\"]}", Event.Type.UNIT_CHILDREN));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 3));

    assertEquals(List.of("u0"), dispatchedKeys());
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testRecoverDerivesHandedBackPartsFromTheReport() throws Exception {
    // An earlier Coordinator, at epoch 1, was told that u0 handed back three parts. It dispatched two of them, and
    // one of those was completed, handing back a part of its own. Then it stopped.
    WorkUnit p1 = unit("p1", 1, 1);
    messenger.log.addAll(List.of(
        created(unit("u0", 1, 1)),
        new Event("connector1", RUN_ID, null, Event.Type.PLANNING_DONE),
        childrenEvent("u0", 1, 1, "x", "p1", "p2", "p3"),
        doneEvent("u0", 1, 1, "x", 3),
        created(p1),
        created(unit("p2", 1, 1)),
        childrenEvent("p1", 1, 1, "y", "p4"),
        doneEvent("p1", 1, 1, "y", 1)));

    CoordinatorPublisher publisher = publisher(2);
    publisher.recover();

    // p3 and p4 were never logged as units. They are known because the reports that named them were.
    assertEquals(2, publisher.numUnitsDone());
    assertEquals(List.of("connector1/p2", "connector1/p3", "connector1/p4"),
        publisher.outstandingUnitIds().stream().sorted().toList());
    assertTrue(messenger.sent.isEmpty());

    // all three are dispatched under the new epoch, as far as there is room: two partitions
    publisher.redispatchOutstandingUnits();
    assertEquals(2, messenger.dispatched.size());
    messenger.dispatched.forEach(unit -> assertEquals(2, unit.epoch()));
    assertEquals("p3", messenger.dispatched.stream().filter(unit -> unit.unitId().endsWith("/p3")).findFirst()
        .map(unit -> unit.payload().get("key").asText()).orElse("p3 is queued"));
    assertEquals(3, publisher.numUnitsOutstanding());

    for (WorkUnit unit : List.copyOf(messenger.dispatched)) {
      publisher.handleEvent(new Event(unit.unitId(), RUN_ID, doneEvent("x", 1, 2, "z", 0).getMessage(), Event.Type.UNIT_DONE));
    }
    assertEquals(3, messenger.dispatched.size());
    publisher.handleEvent(new Event(messenger.dispatched.get(2).unitId(), RUN_ID, doneEvent("x", 1, 2, "z", 0).getMessage(),
        Event.Type.UNIT_DONE));
    assertFalse(publisher.hasOutstandingWork());
    assertEquals(5, publisher.numUnitsDone());
  }

  @Test
  public void testTimeSinceTheWaitLoopLastWentRound() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    // not waiting, so not stuck
    assertEquals(0, publisher.millisSinceWaitIteration());

    publisher.onWaitIteration();
    Thread.sleep(30);
    assertTrue(publisher.millisSinceWaitIteration() >= 30);
    publisher.onWaitIteration();
    assertTrue(publisher.millisSinceWaitIteration() < 30);
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
  public void testOneUnitInFlightPerPartition() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());
    // the work topic has two partitions, so the third unit waits here rather than behind one of the others
    sink.emit("u2", WorkUnit.newPayload());

    assertEquals(List.of("u0", "u1"), dispatchedKeys());
    assertEquals(List.of(0, 1), messenger.partitions);
    assertEquals(2, publisher.numPartitionsBusy());
    assertEquals(3, publisher.numUnitsOutstanding());

    // it goes to whichever partition comes free, and the planner was not held up by the wait
    publisher.handleEvent(unitEvent("u1", 1, 1, Event.Type.UNIT_DONE));
    assertEquals(List.of("u0", "u1", "u2"), dispatchedKeys());
    assertEquals(List.of(0, 1, 1), messenger.partitions);

    // a report on an earlier dispatch of a unit does not free the partition its current dispatch is on
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_FAILED));
    assertEquals(List.of("u0", "u1", "u2", "u0"), dispatchedKeys());
    assertEquals(List.of(0, 1, 1, 0), messenger.partitions);
    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    assertEquals(2, publisher.numPartitionsBusy());

    publisher.handleEvent(unitEvent("u0", 2, 1, Event.Type.UNIT_DONE));
    publisher.handleEvent(unitEvent("u2", 1, 1, Event.Type.UNIT_DONE));
    assertEquals(0, publisher.numPartitionsBusy());
    assertFalse(publisher.hasOutstandingWork());
  }

  @Test
  public void testMostExpensiveUnitsAreDispatchedFirst() throws Exception {
    // an earlier run measured some of the units
    CoordinatorPublisher publisher = new CoordinatorPublisher(CONFIG, messenger, RUN_ID, connector, "test", 1,
        Map.of("connector1/u1", 100L, "connector1/u2", 50L, "connector1/u0", 5L));
    WorkUnitSink sink = publisher.getSink();

    // both partitions busy, so the rest of the plan queues up
    sink.emit("a", WorkUnit.newPayload());
    sink.emit("b", WorkUnit.newPayload());
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());
    sink.emit("u2", WorkUnit.newPayload());
    // the connector's own guess counts for a unit the earlier run did not measure
    sink.emit("u3", WorkUnit.newPayload(), 70);
    sink.emit("u4", WorkUnit.newPayload());
    assertEquals(List.of("a", "b"), dispatchedKeys());

    for (String done : List.of("a", "b", "u1", "u3", "u2")) {
      publisher.handleEvent(unitEvent(done, 1, 1, Event.Type.UNIT_DONE));
    }
    // largest first; the unmeasured, unguessed unit last, after the one planned before it
    assertEquals(List.of("a", "b", "u1", "u3", "u2", "u0", "u4"), dispatchedKeys());
  }

  @Test
  public void testUnitDispatchedAgainKeepsItsCost() throws Exception {
    CoordinatorPublisher publisher = new CoordinatorPublisher(CONFIG, messenger, RUN_ID, connector, "test", 1,
        Map.of("connector1/u1", 100L));
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u1", WorkUnit.newPayload());
    sink.emit("a", WorkUnit.newPayload());
    sink.emit("b", WorkUnit.newPayload());
    sink.emit("c", WorkUnit.newPayload());

    // u1 fails: it goes back to the queue, ahead of b and c, which are cheaper
    publisher.handleEvent(unitEvent("u1", 1, 1, Event.Type.UNIT_FAILED));
    assertEquals(List.of("u1", "a", "u1"), dispatchedKeys());
    assertEquals(2, messenger.dispatched.get(2).attempt());
  }

  @Test
  public void testPlanningIsNotDoneWhilePlannedUnitsAreQueued() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    for (String unitKey : List.of("u0", "u1", "u2", "u3")) {
      sink.emit(unitKey, WorkUnit.newPayload());
    }
    assertEquals(List.of("u0", "u1"), dispatchedKeys());
    assertEquals(2, publisher.numPlannedUnitsQueued());

    // A planned unit is in the log only once dispatched. If planning were recorded as done now and the Coordinator
    // died, its successor would not plan again, and u2 and u3 would never exist.
    Thread planner = new Thread(() -> {
      try {
        publisher.logPlanningDone();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    planner.start();
    planner.join(300);
    assertTrue(planner.isAlive());
    assertFalse(publisher.isPlanningDone());
    assertFalse(messenger.sent.contains("PLANNING_DONE connector1"));

    publisher.handleEvent(unitEvent("u0", 1, 1, Event.Type.UNIT_DONE));
    publisher.handleEvent(unitEvent("u1", 1, 1, Event.Type.UNIT_DONE));
    planner.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(planner.isAlive());
    assertTrue(publisher.isPlanningDone());
    assertEquals(0, publisher.numPlannedUnitsQueued());
    // and the record of it comes after every planned unit's
    assertTrue(messenger.sent.indexOf("UNIT_CREATED connector1/u3") < messenger.sent.indexOf("PLANNING_DONE connector1"));

    // a part handed back is not a planned unit: it is in the log as part of its parent's report
    publisher.handleEvent(childrenEvent("u2", 1, 1, "x", "p1", "p2", "p3"));
    publisher.handleEvent(doneEvent("u2", 1, 1, "x", 3));
    assertEquals(0, publisher.numPlannedUnitsQueued());
    assertTrue(publisher.numUnitsOutstanding() > 2);
  }

  @Test
  public void testPlannedUnitHandedBackMeanwhileIsNotQueuedTwice() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    WorkUnitSink sink = publisher.getSink();
    sink.emit("u0", WorkUnit.newPayload());
    sink.emit("u1", WorkUnit.newPayload());

    // a Crawler hands back a part that the planner then also comes to
    publisher.handleEvent(childrenEvent("u0", 1, 1, "x", "p1"));
    publisher.handleEvent(doneEvent("u0", 1, 1, "x", 1));
    sink.emit("p1", WorkUnit.newPayload());

    publisher.handleEvent(unitEvent("u1", 1, 1, Event.Type.UNIT_DONE));
    publisher.handleEvent(unitEvent("p1", 1, 1, Event.Type.UNIT_DONE));
    assertEquals(List.of("u0", "u1", "p1"), dispatchedKeys());
    assertFalse(publisher.hasOutstandingWork());
    assertEquals(0, publisher.numPartitionsBusy());
  }

  @Test
  public void testDispatchFailureReportedByTheMessengerIsTheUnitsFailure() throws Exception {
    CoordinatorPublisher publisher = publisher(1);
    publisher.getSink().emit("u0", WorkUnit.newPayload());

    // the messenger could not log or send the unit, and says so as a Crawler would
    WorkUnit dispatched = messenger.dispatched.get(0);
    publisher.handleEvent(dispatched.reportEvent(Event.Type.UNIT_FAILED, "coordinator", "could not be sent"));

    assertNull(publisher.failureReason());
    assertEquals(List.of("u0", "u0"), dispatchedKeys());
    assertEquals(2, messenger.dispatched.get(1).attempt());
    // and it counts against the unit's attempts: crawl.maxAttempts is 2
    publisher.handleEvent(messenger.dispatched.get(1).reportEvent(Event.Type.UNIT_FAILED, "coordinator", "could not be sent"));
    assertTrue(publisher.failureReason().contains("could not be sent"));
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
