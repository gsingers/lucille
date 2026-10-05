package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.spec.Spec;

/**
 * Lets any Connector take part in a distributed crawl, as a single unit whose execution is a call to execute().
 * The connector's work is not parallelized, but it runs on a Crawler and is tracked like any other unit.
 */
final class SingleUnitAdapter implements PartitionableConnector {

  static final String UNIT_KEY = "all";

  private final Connector delegate;

  private SingleUnitAdapter(Connector delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns the connector itself if it is configured to be partitioned, and otherwise an adapter around it.
   */
  static PartitionableConnector wrap(Connector connector) {
    if (connector instanceof PartitionableConnector partitionable && partitionable.isPartitioningEnabled()) {
      return partitionable;
    }
    return new SingleUnitAdapter(connector);
  }

  @Override
  public void plan(String runId, WorkUnitSink sink) throws ConnectorException {
    sink.emit(UNIT_KEY, WorkUnit.newPayload());
  }

  @Override
  public void executeUnit(WorkUnit unit, Publisher publisher, UnitContext context) throws ConnectorException {
    delegate.execute(publisher);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getPipelineName() {
    return delegate.getPipelineName();
  }

  @Override
  public boolean requiresCollapsingPublisher() {
    return delegate.requiresCollapsingPublisher();
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    delegate.preExecute(runId);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    delegate.execute(publisher);
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    delegate.postExecute(runId);
  }

  @Override
  public Spec getSpec() {
    return delegate.getSpec();
  }

  @Override
  public String getMessage() {
    return delegate.getMessage();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
