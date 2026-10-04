package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SequenceConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SequenceConnectorTest/config.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new SequenceConnector(config);
    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(100, docs.size());

    // prefix should be applied to doc ids and run_id should be added
    Document doc1 = Document.createFromJson(
        "{\"id\": \"PREFIX10000\", \"run_id\": \"run1\"}");
    Document doc2 = Document.createFromJson(
        "{\"id\": \"PREFIX10001\", \"run_id\": \"run1\"}");
    Document doc3 = Document.createFromJson("{\"id\": \"PREFIX10002\", \"run_id\": \"run1\"}");
    Document doc99 = Document.createFromJson("{\"id\": \"PREFIX10099\", \"run_id\": \"run1\"}");
    assertEquals(doc1, docs.get(0));
    assertEquals(doc2, docs.get(1));
    assertEquals(doc3, docs.get(2));
    assertEquals(doc99, docs.get(99));
  }

  @Test
  public void testLoadConfigWithLongNumDocs() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SequenceConnectorTest/configLargeNumDocs.conf");
    Connector connector = new SequenceConnector(config);
  }

  @Test
  public void testPartitioningIsOptIn() {
    Config config = ConfigFactory.parseString("name: seq, class: x, numDocs: 10");
    assertFalse(new SequenceConnector(config).isPartitioningEnabled());
    assertTrue(new SequenceConnector(config.withFallback(ConfigFactory.parseString("partitioning.unitSize: 4")))
        .isPartitioningEnabled());
    assertThrows(IllegalArgumentException.class,
        () -> new SequenceConnector(config.withFallback(ConfigFactory.parseString("partitioning.unitSize: 0"))));
  }

  @Test
  public void testUnitsCoverTheSequence() throws Exception {
    Config config = ConfigFactory.parseString(
        "name: seq, class: x, numDocs: 10, startWith: 100, docIdPrefix: P, partitioning.unitSize: 4");
    SequenceConnector connector = new SequenceConnector(config);

    Map<String, ObjectNode> units = new LinkedHashMap<>();
    connector.plan("run1", units::put);
    assertEquals(List.of("0-4", "4-8", "8-10"), List.copyOf(units.keySet()));

    // executing the units publishes what execute() does, in the same order
    TestMessenger wholeMessenger = new TestMessenger();
    connector.execute(new PublisherImpl(config, wholeMessenger, "run1", "pipeline1"));

    TestMessenger unitMessenger = new TestMessenger();
    Publisher unitPublisher = new PublisherImpl(config, unitMessenger, "run1", "pipeline1");
    for (Map.Entry<String, ObjectNode> unit : units.entrySet()) {
      connector.executeUnit(unit(unit.getKey(), unit.getValue()), unitPublisher);
    }

    assertEquals(wholeMessenger.getDocsSentForProcessing(), unitMessenger.getDocsSentForProcessing());
    assertEquals("P100", unitMessenger.getDocsSentForProcessing().get(0).getId());
    assertEquals("P109", unitMessenger.getDocsSentForProcessing().get(9).getId());
  }

  @Test
  public void testUnitOutsideTheSequenceIsRefused() throws Exception {
    Config config = ConfigFactory.parseString("name: seq, class: x, numDocs: 10, partitioning.unitSize: 4");
    SequenceConnector connector = new SequenceConnector(config);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    for (ObjectNode payload : List.of(
        WorkUnit.newPayload().put("from", 8).put("to", 1_000_000_000),
        WorkUnit.newPayload().put("from", -5).put("to", 3),
        WorkUnit.newPayload().put("from", 6).put("to", 2),
        WorkUnit.newPayload())) {
      assertThrows(ConnectorException.class, () -> connector.executeUnit(unit("bad", payload), publisher));
    }
    assertTrue(messenger.getDocsSentForProcessing().isEmpty());
  }

  private static WorkUnit unit(String key, ObjectNode payload) {
    return new WorkUnit("run1", "seq", "pipeline1", "seq/" + key, 1, 1, "hash", payload);
  }

}
