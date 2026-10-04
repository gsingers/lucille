package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A slice of one connector's source, small enough to be handed to a single Crawler. The Coordinator creates units from
 * what a {@link PartitionableConnector} plans, and a Crawler executes each one.
 *
 * @param runId the run the unit belongs to
 * @param connectorName the name of the connector, as configured, that planned the unit and will execute it
 * @param pipelineName the pipeline the connector feeds, which determines where Events about the unit are sent
 * @param unitId identifies the unit within the run; stable across attempts, epochs and re-planning
 * @param attempt 1 for the first dispatch, incremented each time the unit is re-dispatched after a reported failure
 * @param epoch the Coordinator epoch that dispatched the unit; a unit from an older epoch than the run's current one is stale
 * @param configHash hash of the connector's config on the Coordinator, so a Crawler holding a different config can refuse the unit
 * @param payload describes the slice; only the connector that planned the unit interprets it
 */
public record WorkUnit(String runId, String connectorName, String pipelineName, String unitId, int attempt, int epoch, String configHash,
                       ObjectNode payload) {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public WorkUnit withAttempt(int newAttempt) {
    return new WorkUnit(runId, connectorName, pipelineName, unitId, newAttempt, epoch, configHash, payload);
  }

  public WorkUnit withEpoch(int newEpoch) {
    return new WorkUnit(runId, connectorName, pipelineName, unitId, attempt, newEpoch, configHash, payload);
  }

  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("WorkUnit could not be serialized.", e);
    }
  }

  public static WorkUnit fromJson(String json) throws JsonProcessingException {
    WorkUnit unit = MAPPER.readValue(json, WorkUnit.class);
    if (unit.runId() == null || unit.connectorName() == null || unit.pipelineName() == null || unit.unitId() == null) {
      throw new IllegalArgumentException("WorkUnit is missing runId, connectorName, pipelineName or unitId.");
    }
    return unit;
  }

  public static ObjectNode newPayload() {
    return MAPPER.createObjectNode();
  }
}
