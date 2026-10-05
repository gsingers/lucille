package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.UnitContext;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A UnitContext for tests that call executeUnit() directly: it keeps what the unit handed back and reported.
 */
public class RecordingUnitContext implements UnitContext {

  /** The payload of each part handed back, by its key, in the order handed back. */
  public final Map<String, ObjectNode> handedBack = new LinkedHashMap<>();
  public long sourceCalls = 0;

  @Override
  public void handBack(String unitKey, ObjectNode payload) {
    handedBack.put(unitKey, payload);
  }

  @Override
  public void addSourceCalls(long calls) {
    sourceCalls += calls;
  }
}
