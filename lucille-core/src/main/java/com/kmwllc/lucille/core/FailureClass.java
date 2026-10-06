package com.kmwllc.lucille.core;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/**
 * What kind of failure a unit of a distributed crawl met, as far as the Coordinator needs to know to respond: a
 * source that refused the request is waited out and tried again, while a connector that threw is a defect.
 */
public enum FailureClass {

  /** The source refused the request because of its rate: HTTP 429, or a throttling error from a cloud SDK. */
  THROTTLED,
  /** The source could not be reached, or answered that it was unavailable: HTTP 5xx, a connection failure. */
  SOURCE_UNAVAILABLE,
  /** The source answered with another error: not found, access denied, a bad request. */
  SOURCE_ERROR,
  /** The unit took too long. */
  TIMEOUT,
  /** Anything else: an exception in the connector or in Lucille. */
  CONNECTOR_ERROR;

  private static final int MAX_CAUSE_DEPTH = 32;

  /** Whether a failure of this class is the source's, and may pass if tried again later. */
  public boolean isSourceFailure() {
    return this == THROTTLED || this == SOURCE_UNAVAILABLE || this == SOURCE_ERROR;
  }

  /** Whether a failure of this class says the source is overloaded, so that trying again at once would add to it. */
  public boolean isOverload() {
    return this == THROTTLED || this == SOURCE_UNAVAILABLE;
  }

  /**
   * Classifies a failure by looking along its chain of causes for the first that says what happened: a
   * {@link SourceException} carries its class, and the common network exceptions are the source being unreachable.
   * A storage client that knows its SDK's exceptions wraps them in a SourceException so that they are classified
   * here without this method knowing the SDK.
   */
  public static FailureClass of(Throwable t) {
    int depth = 0;
    for (Throwable cause = t; cause != null && depth++ < MAX_CAUSE_DEPTH; cause = cause.getCause()) {
      if (cause instanceof SourceException source) {
        return source.getFailureClass();
      }
      if (cause instanceof ConnectException || cause instanceof UnknownHostException
          || cause instanceof NoRouteToHostException || cause instanceof SocketTimeoutException) {
        return SOURCE_UNAVAILABLE;
      }
      if (cause instanceof TimeoutException) {
        return TIMEOUT;
      }
    }
    return CONNECTOR_ERROR;
  }

  /** Returns the innermost cause of a failure, which is usually the one that says what happened. */
  public static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    // bounded, since a chain of causes can be made to loop
    for (int depth = 0; cause.getCause() != null && depth < MAX_CAUSE_DEPTH; depth++) {
      cause = cause.getCause();
    }
    return cause;
  }

  public static FailureClass parse(String name) {
    try {
      return name == null ? null : valueOf(name);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
