package com.kmwllc.lucille.message;

/**
 * API that a Coordinator uses to tell Crawlers whether a run is alive. One instance serves a whole run.
 */
public interface RunControl {

  /**
   * The most recent control record for a run.
   *
   * @param cancelled whether the record was a cancellation rather than a heartbeat
   * @param epoch the Coordinator epoch that wrote the record
   * @param configHash hash of the config the run was started with
   * @param ageMillis how long ago the record was written
   */
  record Status(boolean cancelled, int epoch, String configHash, long ageMillis) {
  }

  /**
   * Returns the most recent control record for the given run, or null if there is none.
   */
  Status latest(String runId) throws Exception;

  /**
   * Announces that the Coordinator of the given run is alive.
   */
  void heartbeat(String runId, int epoch, String configHash) throws Exception;

  /**
   * Announces that Crawlers should discard the given run's units. Sent when a run ends, however it ends.
   */
  void cancel(String runId, int epoch, String configHash, String reason) throws Exception;

  void close();
}
