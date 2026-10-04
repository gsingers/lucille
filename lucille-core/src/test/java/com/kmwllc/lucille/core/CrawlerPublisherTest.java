package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.RunControlTracker.Decision;
import com.kmwllc.lucille.message.CrawlerMessenger;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class CrawlerPublisherTest {

  /** Records what a CrawlerPublisher asks of its messenger. */
  private static class RecordingMessenger implements CrawlerMessenger {

    final RunControlTracker tracker = new RunControlTracker(60_000);
    final List<Document> sent = new ArrayList<>();
    int flushes = 0;
    boolean unitLost = false;
    boolean failSends = false;

    @Override
    public WorkUnit pollWorkUnit() {
      return null;
    }

    @Override
    public void sendForProcessing(Document document, String pipelineName) throws Exception {
      if (failSends) {
        throw new Exception("Kafka send failed");
      }
      assertEquals("pipeline1", pipelineName);
      sent.add(document);
    }

    @Override
    public void sendEvent(Event event, String pipelineName) {
    }

    @Override
    public void flush(String pipelineName) {
      flushes++;
    }

    @Override
    public void ackWorkUnit() {
    }

    @Override
    public void releaseWorkUnit() {
    }

    @Override
    public boolean isUnitLost() {
      return unitLost;
    }

    @Override
    public RunControlTracker getRunControlTracker() {
      return tracker;
    }

    @Override
    public void close() {
    }
  }

  private static final WorkUnit UNIT =
      new WorkUnit("run1", "connector1", "pipeline1", "connector1/u0", 1, 1, "hash", WorkUnit.newPayload());

  private RecordingMessenger messenger;

  @Before
  public void setUp() {
    messenger = new RecordingMessenger();
    messenger.tracker.onHeartbeat("run1", 1, System.currentTimeMillis());
  }

  private CrawlerPublisher publisher(boolean collapsing) {
    return new CrawlerPublisher(messenger, UNIT, "pipeline1", collapsing);
  }

  @Test
  public void testPublish() throws Exception {
    CrawlerPublisher publisher = publisher(false);
    publisher.publish(Document.create("doc1"));
    publisher.publish(Document.create("doc2"));
    publisher.flush();

    assertEquals(2, messenger.sent.size());
    // each Document is stamped with the run of the unit, as PublisherImpl stamps the run it was created for
    assertEquals("run1", messenger.sent.get(0).getRunId());
    assertEquals(2, publisher.numPublished());
    assertEquals(2, publisher.numReceived());
    assertEquals(1, messenger.flushes);
    assertNull(publisher.getAbortReason());
    assertFalse(publisher.isUnitLost());
  }

  @Test
  public void testCollapsing() throws Exception {
    CrawlerPublisher publisher = publisher(true);
    Document first = Document.create("doc1");
    first.setField("field", "a");
    Document second = Document.create("doc1");
    second.setField("field", "b");

    publisher.publish(first);
    publisher.publish(second);
    publisher.publish(Document.create("doc2"));
    // the last Document is held back until it is known that no more with its ID will follow
    assertEquals(1, messenger.sent.size());
    publisher.flush();

    assertEquals(2, messenger.sent.size());
    assertEquals(List.of("a", "b"), messenger.sent.get(0).getStringList("field"));
    assertEquals(3, publisher.numReceived());
    assertEquals(2, publisher.numPublished());
  }

  @Test
  public void testSendFailureReachesTheConnector() {
    messenger.failSends = true;
    CrawlerPublisher publisher = publisher(false);

    assertThrows(Exception.class, () -> publisher.publish(Document.create("doc1")));
    assertEquals(0, publisher.numPublished());
    // the unit failed in the ordinary way; it was not abandoned
    assertNull(publisher.getAbortReason());
  }

  @Test
  public void testPublishStopsWhenTheRunIsCancelled() throws Exception {
    CrawlerPublisher publisher = publisher(false);
    publisher.publish(Document.create("doc1"));

    messenger.tracker.onCancel("run1", 1);
    assertThrows(ConnectorException.class, () -> publisher.publish(Document.create("doc2")));
    assertEquals(Decision.CANCELLED, publisher.getAbortReason());
    assertEquals(1, messenger.sent.size());
  }

  @Test
  public void testPublishStopsWhenANewerEpochTakesOver() {
    CrawlerPublisher publisher = publisher(false);
    messenger.tracker.onHeartbeat("run1", 2, System.currentTimeMillis());

    assertThrows(ConnectorException.class, () -> publisher.publish(Document.create("doc1")));
    assertEquals(Decision.STALE, publisher.getAbortReason());
    assertTrue(messenger.sent.isEmpty());
  }

  @Test
  public void testPublishStopsWhenTheCoordinatorGoesSilent() {
    // a tracker for which any heartbeat is already too old
    RunControlTracker impatient = new RunControlTracker(-1);
    impatient.onHeartbeat("run1", 1, System.currentTimeMillis());
    CrawlerMessenger impatientMessenger = new RecordingMessenger() {
      @Override
      public RunControlTracker getRunControlTracker() {
        return impatient;
      }
    };
    CrawlerPublisher publisher = new CrawlerPublisher(impatientMessenger, UNIT, "pipeline1", false);

    assertThrows(ConnectorException.class, () -> publisher.publish(Document.create("doc1")));
    assertEquals(Decision.ORPHANED, publisher.getAbortReason());
  }

  @Test
  public void testPublishStopsWhenTheUnitIsReassigned() {
    CrawlerPublisher publisher = publisher(false);
    messenger.unitLost = true;

    assertThrows(ConnectorException.class, () -> publisher.publish(Document.create("doc1")));
    assertTrue(publisher.isUnitLost());
    assertTrue(messenger.sent.isEmpty());
  }

  @Test
  public void testTrackingIsLeftToTheCoordinator() throws Exception {
    CrawlerPublisher publisher = publisher(false);
    publisher.publish(Document.create("doc1"));

    assertFalse(publisher.hasPending());
    assertEquals(0, publisher.numPending());
    assertEquals(0, publisher.numCreated() + publisher.numSucceeded() + publisher.numFailed() + publisher.numDropped());
    assertThrows(UnsupportedOperationException.class, () -> publisher.handleEvent(null));
    assertThrows(UnsupportedOperationException.class, () -> publisher.waitForCompletion(null, 0));
    assertThrows(UnsupportedOperationException.class, publisher::pause);
    assertThrows(UnsupportedOperationException.class, publisher::resume);
    publisher.preClose();
    publisher.close();
  }
}
