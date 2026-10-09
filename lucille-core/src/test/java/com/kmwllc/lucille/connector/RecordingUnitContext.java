package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.FailureClass;
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
  public long refusedCalls = 0;
  public FailureClass errorClass;
  public String errorCause;
  /** The keys of the parts handed back after a source error had been recorded. */
  public final java.util.Set<String> handedBackAfterError = new java.util.LinkedHashSet<>();
  /** What the unit asked to have run at each progress report. */
  public final java.util.List<Runnable> progressActions = new java.util.ArrayList<>();
  public Long timePastBound;
  public long maxDirectoryPages = 0;

  @Override
  public void onProgress(Runnable action) {
    progressActions.add(action);
  }

  @Override
  public void recordTimePastBound(long millis) {
    timePastBound = millis;
  }

  @Override
  public void recordDirectoryPages(long pages) {
    maxDirectoryPages = Math.max(maxDirectoryPages, pages);
  }

  @Override
  public void handBack(String unitKey, ObjectNode payload) {
    handedBack.put(unitKey, payload);
    if (errorClass != null) {
      handedBackAfterError.add(unitKey);
    }
  }

  @Override
  public void addSourceCalls(long calls) {
    sourceCalls += calls;
  }

  @Override
  public void addRefusedCalls(long calls) {
    refusedCalls += calls;
  }

  @Override
  public void recordSourceError(FailureClass failureClass, String cause) {
    errorClass = failureClass;
    errorCause = cause;
  }
}
