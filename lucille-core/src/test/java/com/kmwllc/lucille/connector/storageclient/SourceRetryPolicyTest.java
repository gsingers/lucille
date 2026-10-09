package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class SourceRetryPolicyTest {

  @Test
  public void testDefaultsAndConfig() {
    SourceRetryPolicy defaults = SourceRetryPolicy.fromConfig(ConfigFactory.empty());
    assertEquals(180_000, defaults.maxMillis());
    assertEquals(20_000, defaults.capMillis());

    SourceRetryPolicy configured = SourceRetryPolicy.fromConfig(ConfigFactory.parseString("sourceRetrySecs: 5, sourceRetryCapSecs: 2"));
    assertEquals(5000, configured.maxMillis());
    assertEquals(2000, configured.capMillis());

    // numbers substituted from the environment arrive as text
    assertEquals(7000, SourceRetryPolicy.fromConfig(ConfigFactory.parseString("sourceRetrySecs: \"7\"")).maxMillis());

    assertThrows(IllegalArgumentException.class, () -> SourceRetryPolicy.fromConfig(ConfigFactory.parseString("sourceRetrySecs: -1")));
    assertThrows(IllegalArgumentException.class, () -> SourceRetryPolicy.fromConfig(ConfigFactory.parseString("sourceRetryCapSecs: 0")));
  }

  @Test
  public void testWaitsGrowAndAreCapped() {
    SourceRetryPolicy policy = new SourceRetryPolicy(60_000, 8000);
    long now = System.currentTimeMillis();

    for (int i = 0; i < 20; i++) {
      long first = policy.nextWaitMillis(1, now);
      long second = policy.nextWaitMillis(2, now);
      long fourth = policy.nextWaitMillis(4, now);
      long tenth = policy.nextWaitMillis(10, now);
      // between half the ceiling and the ceiling: 1 s, 2 s, 8 s, and 8 s again once capped
      assertTrue(String.valueOf(first), first > 500 && first <= 1000);
      assertTrue(String.valueOf(second), second > 1000 && second <= 2000);
      assertTrue(String.valueOf(fourth), fourth > 4000 && fourth <= 8000);
      assertTrue(String.valueOf(tenth), tenth > 4000 && tenth <= 8000);
    }
  }

  @Test
  public void testGivesUpOnceTheHorizonHasPassed() {
    SourceRetryPolicy policy = new SourceRetryPolicy(1000, 500);
    assertTrue(policy.nextWaitMillis(1, System.currentTimeMillis()) > 0);
    assertEquals(-1, policy.nextWaitMillis(3, System.currentTimeMillis() - 1001));
    // no retries at all
    assertEquals(-1, SourceRetryPolicy.NONE.nextWaitMillis(1, System.currentTimeMillis()));
  }
}
