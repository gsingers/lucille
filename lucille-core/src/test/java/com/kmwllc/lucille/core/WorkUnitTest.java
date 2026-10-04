package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class WorkUnitTest {

  private static final WorkUnit UNIT = new WorkUnit("run1", "connector1", "pipeline1", "connector1/u0", 2, 3, "hash",
      WorkUnit.newPayload().put("prefix", "docs/2024/").put("recursive", true));

  @Test
  public void testJsonRoundTrip() throws Exception {
    WorkUnit parsed = WorkUnit.fromJson(UNIT.toJson());

    assertEquals(UNIT, parsed);
    assertEquals("docs/2024/", parsed.payload().get("prefix").asText());
  }

  @Test
  public void testWithAttemptAndEpoch() {
    WorkUnit retried = UNIT.withAttempt(5);
    assertEquals(5, retried.attempt());
    assertEquals(3, retried.epoch());
    assertEquals(UNIT.unitId(), retried.unitId());
    assertEquals(UNIT.payload(), retried.payload());

    WorkUnit redispatched = UNIT.withEpoch(7);
    assertEquals(7, redispatched.epoch());
    assertEquals(2, redispatched.attempt());
  }

  @Test
  public void testIncompleteUnitIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> WorkUnit.fromJson("{\"runId\": \"run1\", \"unitId\": \"u0\"}"));
    assertThrows(Exception.class, () -> WorkUnit.fromJson("not json"));
    // a field this version does not know of is not quietly dropped
    assertThrows(Exception.class, () -> WorkUnit.fromJson(UNIT.toJson().replace("\"attempt\"", "\"unknown\":1,\"attempt\"")));
  }
}
