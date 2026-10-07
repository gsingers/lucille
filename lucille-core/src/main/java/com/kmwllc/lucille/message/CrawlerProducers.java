package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What a Crawler sends to Kafka, and on which producer.
 *
 * Documents go to the source topic on one or more document producers. A producer has one thread sending to the
 * brokers, and a Crawler whose traversal finds Documents faster than that thread can send them blocks its traversal
 * threads waiting for room in the producer's buffer; several producers give several such threads. A Document always
 * goes to the same producer, chosen by its ID, so that sends of the same Document stay in order.
 *
 * Each Document's CREATE Event goes to the run's event topic once the source topic has accepted the Document, on a
 * producer of its own. Unit reports (progress, hand-backs, completion) go to the same topic on a third producer, so
 * that sending one, which waits for it to be accepted, never waits behind a backlog of CREATEs. A unit's completion
 * is sent only after {@link #flush} has seen every one of its CREATEs accepted, so it still follows them in the topic.
 */
class CrawlerProducers implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CrawlerProducers.class);

  private final Config config;
  private final List<Producer<String, Document>> documentProducers;
  private final Producer<String, String> createProducer;
  private final Producer<String, String> reportProducer;

  // Documents the source topic has accepted, each awaiting its CREATE Event. Filled by the producers' callback threads.
  private final ConcurrentLinkedQueue<Event> pendingCreates = new ConcurrentLinkedQueue<>();
  private final AtomicReference<Exception> sendException = new AtomicReference<>();

  CrawlerProducers(Config config, List<Producer<String, Document>> documentProducers,
      Producer<String, String> createProducer, Producer<String, String> reportProducer) {
    if (documentProducers.isEmpty()) {
      throw new IllegalArgumentException("A Crawler needs at least one document producer.");
    }
    this.config = config;
    this.documentProducers = List.copyOf(documentProducers);
    this.createProducer = createProducer;
    this.reportProducer = reportProducer;
  }

  /** Creates the producers from the config: the given number for Documents, one for CREATEs, one for reports. */
  static CrawlerProducers create(Config config, int numDocumentProducers) {
    Producer<String, String> createProducer = KafkaUtils.createEventProducer(config);
    if (createProducer == null) {
      throw new IllegalArgumentException("A distributed crawl requires Events; kafka.events cannot be false.");
    }
    List<Producer<String, Document>> documentProducers = new ArrayList<>();
    for (int i = 0; i < numDocumentProducers; i++) {
      documentProducers.add(KafkaUtils.createDocumentProducer(config));
    }
    return new CrawlerProducers(config, documentProducers, createProducer, KafkaUtils.createEventProducer(config));
  }

  /**
   * Waits for Documents still being sent, then forgets their outcome and any CREATEs not yet sent. Called before a
   * new unit is taken, so that the last unit's sends are not taken for the new one's.
   */
  void startUnit() {
    documentProducers.forEach(Producer::flush);
    sendException.set(null);
    pendingCreates.clear();
  }

  void sendForProcessing(Document document, String pipelineName) throws Exception {
    checkException();
    sendPendingCreates(pipelineName);

    Event create = new Event(document, null, Event.Type.CREATE);
    producerFor(document.getId()).send(
        new ProducerRecord<>(KafkaUtils.getSourceTopicName(pipelineName, config), document.getId(), document),
        (metadata, exception) -> {
          if (exception != null) {
            log.error("Kafka send failed for document: {}", create.getDocumentId(), exception);
            sendException.compareAndSet(null, exception);
          } else {
            pendingCreates.add(create);
          }
        });
  }

  private Producer<String, Document> producerFor(String documentId) {
    return documentProducers.get(Math.floorMod(documentId.hashCode(), documentProducers.size()));
  }

  private void sendPendingCreates(String pipelineName) {
    Event create;
    while ((create = pendingCreates.poll()) != null) {
      String eventTopic = KafkaUtils.getEventTopicName(config, pipelineName, create.getRunId());
      String docId = create.getDocumentId();
      createProducer.send(new ProducerRecord<>(eventTopic, docId, create.toString()), (metadata, exception) -> {
        if (exception != null) {
          log.error("Kafka send failed for CREATE event of document: {}", docId, exception);
          sendException.compareAndSet(null, exception);
        }
      });
    }
  }

  /** Waits until every Document sent so far, and its CREATE, has been accepted. */
  void flush(String pipelineName) throws Exception {
    documentProducers.forEach(Producer::flush);
    sendPendingCreates(pipelineName);
    createProducer.flush();
    checkException();
  }

  private void checkException() throws Exception {
    Exception e = sendException.get();
    if (e != null) {
      throw new Exception("Kafka send failed", e);
    }
  }

  /** Sends a unit report and waits for it to be accepted. */
  void sendEvent(Event event, String pipelineName) throws Exception {
    String eventTopic = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    Future<RecordMetadata> sent = reportProducer.send(new ProducerRecord<>(eventTopic, event.getDocumentId(), event.toString()));
    // sent at once: left to itself the producer holds a record back for linger.ms in case more follow
    reportProducer.flush();
    sent.get();
  }

  @Override
  public void close() {
    for (Producer<String, Document> producer : documentProducers) {
      KafkaCoordinatorMessenger.closeQuietly(producer, "document producer");
    }
    KafkaCoordinatorMessenger.closeQuietly(createProducer, "CREATE event producer");
    KafkaCoordinatorMessenger.closeQuietly(reportProducer, "unit report producer");
  }
}
