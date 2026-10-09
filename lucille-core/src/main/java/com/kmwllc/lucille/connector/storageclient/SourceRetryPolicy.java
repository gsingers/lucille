package com.kmwllc.lucille.connector.storageclient;

import com.typesafe.config.Config;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * How long a storage client goes on retrying a request that its source refused or could not answer, and how it
 * spaces the retries: exponential backoff with full jitter, so that many clients refused together do not all come
 * back together.
 *
 * A source that throttles does so in bursts of a minute or more; the SDKs' own retries last seconds and give up
 * inside the burst. This is the longer horizon.
 *
 * @param maxMillis how long, in all, to go on retrying one request. 0 means no retries.
 * @param capMillis the longest single wait.
 */
public record SourceRetryPolicy(long maxMillis, long capMillis) {

  public static final String RETRY_SECS = "sourceRetrySecs";
  public static final String RETRY_CAP_SECS = "sourceRetryCapSecs";
  public static final int DEFAULT_RETRY_SECS = 180;
  public static final int DEFAULT_RETRY_CAP_SECS = 20;
  private static final long FIRST_WAIT_MILLIS = 1000;

  public static final SourceRetryPolicy NONE = new SourceRetryPolicy(0, 0);

  /** Reads <code>sourceRetrySecs</code> and <code>sourceRetryCapSecs</code> from a connector's config. */
  public static SourceRetryPolicy fromConfig(Config config) {
    int retrySecs = config.hasPath(RETRY_SECS) ? config.getInt(RETRY_SECS) : DEFAULT_RETRY_SECS;
    int capSecs = config.hasPath(RETRY_CAP_SECS) ? config.getInt(RETRY_CAP_SECS) : DEFAULT_RETRY_CAP_SECS;
    if (retrySecs < 0 || capSecs < 1) {
      throw new IllegalArgumentException(RETRY_SECS + " cannot be negative and " + RETRY_CAP_SECS + " must be at least 1.");
    }
    return new SourceRetryPolicy(TimeUnit.SECONDS.toMillis(retrySecs), TimeUnit.SECONDS.toMillis(capSecs));
  }

  /**
   * Returns how long to wait before the given retry (the first retry is 1), or -1 if the request should be given up
   * on because retrying has gone on for longer than the policy allows.
   *
   * @param firstFailureMillis when the request first failed, by System.currentTimeMillis().
   */
  public long nextWaitMillis(int retry, long firstFailureMillis) {
    if (maxMillis <= 0 || System.currentTimeMillis() - firstFailureMillis >= maxMillis) {
      return -1;
    }
    long ceiling = Math.min(capMillis, FIRST_WAIT_MILLIS << Math.min(retry - 1, 20));
    // jittered, between half the ceiling and the ceiling, so refused clients spread out instead of returning in step
    return ThreadLocalRandom.current().nextLong(ceiling / 2 + 1, ceiling + 1);
  }
}
