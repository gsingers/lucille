package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class FailureClassTest {

  @Test
  public void testSourceExceptionIsClassifiedWhereverItIsInTheChain() {
    SourceException throttled = new SourceException(FailureClass.THROTTLED, "429", new IOException("Too many requests"));
    // a connector wraps what its storage client threw, and Lucille wraps that again
    Exception wrapped = new ConnectorException("Error publishing document",
        new ConnectorException("Error occurred while traversing s3://bucket/", throttled));

    assertEquals(FailureClass.THROTTLED, FailureClass.of(wrapped));
    assertEquals(FailureClass.THROTTLED, FailureClass.of(throttled));
    // the innermost cause is what says what happened
    assertSame(throttled.getCause(), FailureClass.rootCause(wrapped));
  }

  @Test
  public void testNetworkFailuresAreTheSourceBeingUnavailable() {
    assertEquals(FailureClass.SOURCE_UNAVAILABLE, FailureClass.of(new RuntimeException(new ConnectException("refused"))));
    assertEquals(FailureClass.SOURCE_UNAVAILABLE, FailureClass.of(new UnknownHostException("no such host")));
    assertEquals(FailureClass.TIMEOUT, FailureClass.of(new ConnectorException("x", new TimeoutException())));
  }

  @Test
  public void testAnythingElseIsTheConnectors() {
    assertEquals(FailureClass.CONNECTOR_ERROR, FailureClass.of(new NullPointerException()));
    assertEquals(FailureClass.CONNECTOR_ERROR, FailureClass.of(new ConnectorException("bad payload")));
    Throwable self = new RuntimeException("x");
    assertSame(self, FailureClass.rootCause(self));
  }

  @Test
  public void testWhichClassesAreTheSources() {
    assertTrue(FailureClass.THROTTLED.isSourceFailure());
    assertTrue(FailureClass.THROTTLED.isOverload());
    assertTrue(FailureClass.SOURCE_UNAVAILABLE.isOverload());
    assertTrue(FailureClass.SOURCE_ERROR.isSourceFailure());
    assertFalse(FailureClass.SOURCE_ERROR.isOverload());
    assertFalse(FailureClass.CONNECTOR_ERROR.isSourceFailure());
    assertFalse(FailureClass.TIMEOUT.isSourceFailure());
  }

  @Test
  public void testParse() {
    assertEquals(FailureClass.THROTTLED, FailureClass.parse("THROTTLED"));
    assertNull(FailureClass.parse("no such class"));
    assertNull(FailureClass.parse(null));
  }
}
