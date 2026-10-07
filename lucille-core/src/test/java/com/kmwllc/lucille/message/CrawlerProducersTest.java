package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class CrawlerProducersTest {

  private static final Config CONFIG = ConfigFactory.parseString("kafka.bootstrapServers: \"localhost:9092\"");

  private static MockProducer<String, Document> documentProducer() {
    return new MockProducer<>(true, null, new StringSerializer(), new KafkaDocumentSerializer());
  }

  private static Document doc(String id) {
    return Document.create(id, "run1");
  }

  @Test
  public void testDocumentsAreSpreadOverTheProducersByID() throws Exception {
    List<MockProducer<String, Document>> producers = List.of(documentProducer(), documentProducer(), documentProducer());
    MockProducer<String, String> creates = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    MockProducer<String, String> reports = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    CrawlerProducers crawlerProducers = new CrawlerProducers(CONFIG, new ArrayList<>(producers), creates, reports);

    for (int i = 0; i < 300; i++) {
      crawlerProducers.sendForProcessing(doc("doc" + i), "pipeline1");
    }
    // the same Document again goes to the same producer, so its sends stay in order
    crawlerProducers.sendForProcessing(doc("doc7"), "pipeline1");
    crawlerProducers.flush("pipeline1");

    int total = 0;
    for (MockProducer<String, Document> producer : producers) {
      assertTrue("a producer was given nothing", producer.history().size() > 50);
      total += producer.history().size();
    }
    assertEquals(301, total);
    long holdingDoc7 = producers.stream()
        .filter(producer -> producer.history().stream().anyMatch(record -> record.key().equals("doc7"))).count();
    assertEquals(1, holdingDoc7);

    // every Document accepted has its CREATE, on the CREATE producer and not the report producer
    Set<String> created = creates.history().stream().map(ProducerRecord::key).collect(Collectors.toSet());
    assertEquals(300, created.size());
    assertTrue(reports.history().isEmpty());
  }

  @Test
  public void testUnitReportsDoNotWaitForCreates() throws Exception {
    // the CREATE producer is backed up: nothing it is given completes
    MockProducer<String, String> creates = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
    MockProducer<String, String> reports = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    CrawlerProducers crawlerProducers = new CrawlerProducers(CONFIG, new ArrayList<>(List.of(documentProducer())), creates, reports);
    crawlerProducers.sendForProcessing(doc("doc1"), "pipeline1");
    crawlerProducers.sendForProcessing(doc("doc2"), "pipeline1");
    assertEquals(1, creates.history().size());

    // a unit report goes out without flushing the CREATEs queued ahead of it
    crawlerProducers.sendEvent(new Event("connector1/u0", "run1", "{}", Event.Type.UNIT_PROGRESS), "pipeline1");
    assertEquals(1, reports.history().size());
    assertEquals("pipeline1_event_run1", reports.history().get(0).topic());
    // the CREATE is still waiting: completing it now finds it there
    assertTrue(creates.completeNext());
  }
}
