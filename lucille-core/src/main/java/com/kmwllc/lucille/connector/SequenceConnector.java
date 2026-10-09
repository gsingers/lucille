package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PartitionableConnector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.UnitContext;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.core.WorkUnitSink;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces a sequence of empty Documents with numeric IDs.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>numDocs (Long, Required) : Total number of Documents to create.</li>
 *   <li>startWith (Int, Optional) : First ID value to use. Defaults to 0.</li>
 *   <li>partitioning.unitSize (Int, Optional) : In a distributed crawl, split the sequence into work units of this many
 *   Documents each. Without it, a distributed crawl produces the whole sequence as one unit.</li>
 * </ul>
 */
public class SequenceConnector extends AbstractConnector implements PartitionableConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredNumber("numDocs")
      .optionalNumber("startWith")
      .optionalParent(SpecBuilder.parent("partitioning").requiredNumber("unitSize").build()).build();

  private static final Logger log = LoggerFactory.getLogger(SequenceConnector.class);
  private final long numDocs;
  private final int startWith;

  public SequenceConnector(Config config) {
    super(config);

    this.numDocs = config.getLong("numDocs");
    this.startWith = config.hasPath("startWith") ? config.getInt("startWith") : 0;

    if (isPartitioningEnabled() && config.getInt("partitioning.unitSize") < 1) {
      throw new IllegalArgumentException("partitioning.unitSize must be at least 1.");
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    publishRange(publisher, 0, numDocs);
  }

  // Publishes the Documents at positions [from, to) of the sequence.
  private void publishRange(Publisher publisher, long from, long to) throws ConnectorException {
    for (long i = from; i < to; i++) {
      Document doc = Document.create(createDocId(Long.toString(i + startWith)));
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Error creating or publishing document", e);
      }
    }
  }

  @Override
  public boolean isPartitioningEnabled() {
    return config.hasPath("partitioning");
  }

  @Override
  public void plan(String runId, WorkUnitSink sink) throws ConnectorException {
    int unitSize = config.getInt("partitioning.unitSize");

    for (long from = 0; from < numDocs; from += unitSize) {
      long to = Math.min(from + unitSize, numDocs);
      sink.emit(from + "-" + to, WorkUnit.newPayload().put("from", from).put("to", to));
    }
  }

  @Override
  public void executeUnit(WorkUnit unit, Publisher publisher, UnitContext context) throws ConnectorException {
    long from = unit.payload().path("from").asLong(-1);
    long to = unit.payload().path("to").asLong(-1);

    if (from < 0 || to < from || to > numDocs) {
      throw new ConnectorException("Work unit " + unit.unitId() + " does not describe a range within the sequence.");
    }

    publishRange(publisher, from, to);
  }
}
