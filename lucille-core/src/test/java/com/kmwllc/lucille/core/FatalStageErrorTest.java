package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * DEMONSTRATION of the fatal-stage-error failure mode (the "hostile document blows up a
 * parser" scenario, e.g. an OutOfMemoryError thrown while parsing). These tests PASS by
 * asserting the CURRENT behavior; the fatal-error test documents a defect, and a future fix
 * (e.g. catching Throwable around pipeline processing, or supervising worker threads)
 * should FLIP its assertions.
 *
 * <p>The contrast demonstrated here:
 * <ul>
 *   <li>A Stage that throws a {@link StageException} is HANDLED: Stage.apply wraps it in a
 *   RuntimeException, Worker.run's {@code catch (Exception)} around pipeline processing
 *   sends a FAIL event for the document, commits offsets, and keeps polling. The worker
 *   survives and processes subsequent documents.</li>
 *   <li>A Stage that throws an {@link Error} (e.g. OutOfMemoryError from a parser choking
 *   on a hostile file) is NOT handled: Error is not an Exception, so it sails past both
 *   catch blocks in Worker.run and escapes the thread entirely. The worker thread dies with
 *   an uncaught Throwable: no FAIL event is sent, nothing is committed or closed, and any
 *   documents queued behind the hostile one are never consumed by this worker.</li>
 * </ul>
 */
public class FatalStageErrorTest {

  /**
   * Simulates a parser exploding on a hostile document. Does not actually allocate memory;
   * just throws the Error a real parser (e.g. a document-extraction library) would.
   */
  public static class SimulatedFatalErrorStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public SimulatedFatalErrorStage(Config config) {
      super(config);
    }

    @Override
    public Iterator<Document> processDocument(Document doc) throws StageException {
      throw new OutOfMemoryError("simulated parser failure");
    }
  }

  /**
   * Simulates an ordinary, recoverable stage failure.
   */
  public static class RecoverableFailureStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public RecoverableFailureStage(Config config) {
      super(config);
    }

    @Override
    public Iterator<Document> processDocument(Document doc) throws StageException {
      throw new StageException("simulated recoverable stage failure");
    }
  }

  private static final String CONFIG =
      "pipelines: ["
          + "{name: \"fatal\", stages: [{class: \"com.kmwllc.lucille.core.FatalStageErrorTest$SimulatedFatalErrorStage\"}]},"
          + "{name: \"recoverable\", stages: [{class: \"com.kmwllc.lucille.core.FatalStageErrorTest$RecoverableFailureStage\"}]}"
          + "]";

  /**
   * The defect: an Error thrown by a stage kills the worker thread outright. No FAIL event
   * is sent for the document that triggered it, and documents queued behind it are stranded.
   */
  @Test(timeout = 30000)
  public void testErrorFromStageKillsWorkerThread() throws Exception {
    Config config = ConfigFactory.parseString(CONFIG);
    TestMessenger messenger = new TestMessenger();
    messenger.sendForProcessing(Document.create("doc1", "run1"));
    messenger.sendForProcessing(Document.create("doc2", "run1"));

    Worker worker = new Worker(config, messenger, "run1", "fatal", "fatal");
    WorkerThread workerThread = new WorkerThread(worker, "fatal-stage-worker");
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    workerThread.setUncaughtExceptionHandler((thread, throwable) -> uncaught.set(throwable));
    workerThread.start();

    // the thread dies on its own; nobody calls terminate()
    workerThread.join(20000);
    assertFalse("worker thread should have died on the Error from the stage",
        workerThread.isAlive());
    assertTrue("the thread should have died with the simulated OutOfMemoryError, but got: "
            + uncaught.get(),
        uncaught.get() instanceof OutOfMemoryError);
    assertEquals("simulated parser failure", uncaught.get().getMessage());

    // no FAIL event was sent for doc1 -- in a real run, the Publisher would wait forever
    // for an event that never comes
    assertEquals(0, messenger.getSentEvents().size());
    assertEquals(0, messenger.getDocsSentForIndexing().size());

    // doc2 was never consumed: it is still sitting on the source queue behind the
    // document that killed the worker
    Document stranded = messenger.pollDocToProcess();
    assertNotNull("doc2 should still be waiting on the source queue", stranded);
    assertEquals("doc2", stranded.getId());
  }

  /**
   * The control case: a StageException is handled gracefully. Each failing document gets a
   * FAIL event and the worker keeps going -- it survives the first failure and processes
   * (and fails) the second document too.
   */
  @Test(timeout = 30000)
  public void testStageExceptionIsHandledAndWorkerSurvives() throws Exception {
    Config config = ConfigFactory.parseString(CONFIG);
    TestMessenger messenger = new TestMessenger();
    messenger.sendForProcessing(Document.create("doc1", "run1"));
    messenger.sendForProcessing(Document.create("doc2", "run1"));

    Worker worker = new Worker(config, messenger, "run1", "recoverable", "recoverable");
    WorkerThread workerThread = new WorkerThread(worker, "recoverable-stage-worker");
    workerThread.start();

    // wait (bounded) for both documents to be processed and failed
    long deadline = System.currentTimeMillis() + 20000;
    while (messenger.getSentEvents().size() < 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }

    assertEquals(2, messenger.getSentEvents().size());
    assertEquals(Event.Type.FAIL, messenger.getSentEvents().get(0).getType());
    assertEquals("doc1", messenger.getSentEvents().get(0).getDocumentId());
    assertEquals(Event.Type.FAIL, messenger.getSentEvents().get(1).getType());
    assertEquals("doc2", messenger.getSentEvents().get(1).getDocumentId());
    assertEquals(0, messenger.getDocsSentForIndexing().size());

    // unlike the Error case, the worker survived both failures and is still polling
    assertTrue("worker thread should still be alive after handled stage failures",
        workerThread.isAlive());

    workerThread.terminate();
    workerThread.join(10000);
    assertFalse(workerThread.isAlive());
  }
}
