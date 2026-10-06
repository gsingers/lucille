package com.kmwllc.lucille.core;

/**
 * A failure of the source a connector reads from, classified so that a distributed crawl can respond to it: see
 * {@link FailureClass}. Storage clients throw it, or wrap their SDK's exceptions in it, once they have given up on a
 * request.
 */
public class SourceException extends ConnectorException {

  private final FailureClass failureClass;

  public SourceException(FailureClass failureClass, String message, Throwable cause) {
    super(message, cause);
    this.failureClass = failureClass;
  }

  public FailureClass getFailureClass() {
    return failureClass;
  }
}
