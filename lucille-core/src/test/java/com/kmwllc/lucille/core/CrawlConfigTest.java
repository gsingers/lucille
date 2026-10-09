package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.admin.NewTopic;
import java.io.File;
import java.nio.file.Files;
import org.junit.Test;

public class CrawlConfigTest {

  @Test
  public void testSourceConcurrencySettings() {
    CrawlConfig off = new CrawlConfig(ConfigFactory.empty());
    assertNull(off.maxSourceConcurrency);

    CrawlConfig defaults = new CrawlConfig(ConfigFactory.parseString("crawl.maxSourceConcurrency: 2400"));
    assertEquals(Integer.valueOf(2400), defaults.maxSourceConcurrency);
    assertEquals(600, defaults.initialSourceConcurrency);
    assertEquals(240, defaults.sourceConcurrencyStep);
    assertEquals(120, defaults.sourceConcurrencyHoldSecs);

    CrawlConfig set = new CrawlConfig(ConfigFactory.parseString(
        "crawl { maxSourceConcurrency: \"100\", initialSourceConcurrency: 10, sourceConcurrencyStep: 5, sourceConcurrencyHoldSecs: 30 }"));
    assertEquals(10, set.initialSourceConcurrency);
    assertEquals(5, set.sourceConcurrencyStep);
    assertEquals(30, set.sourceConcurrencyHoldSecs);

    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString(
        "crawl { maxSourceConcurrency: 10, initialSourceConcurrency: 20 }")));
    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString("crawl.maxSourceConcurrency: 0")));
  }

  @Test
  public void testKafkaPollMustBeShorterThanTheOrphanTimeout() {
    // a Coordinator waits a poll at a time, and one that has not gone round its loop in orphanTimeoutSecs is stuck
    new CrawlConfig(ConfigFactory.parseString("kafka.pollIntervalMs: 5000, crawl.orphanTimeoutSecs: 15"));
    assertThrows(IllegalArgumentException.class,
        () -> new CrawlConfig(ConfigFactory.parseString("kafka.pollIntervalMs: 15001, crawl.orphanTimeoutSecs: 15")));
    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString("kafka.pollIntervalMs: 0")));
  }

  @Test
  public void testThrottleSettings() {
    CrawlConfig defaults = new CrawlConfig(ConfigFactory.empty());
    assertEquals(20, defaults.maxThrottledAttempts);
    assertEquals(10, defaults.throttleBackoffSecs);
    assertEquals(120, defaults.throttleBackoffCapSecs);

    CrawlConfig set = new CrawlConfig(ConfigFactory.parseString(
        "crawl { maxThrottledAttempts: \"5\", throttleBackoffSecs: 2, throttleBackoffCapSecs: 4 }"));
    assertEquals(5, set.maxThrottledAttempts);
    assertEquals(2, set.throttleBackoffSecs);
    assertEquals(4, set.throttleBackoffCapSecs);

    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString(
        "crawl { throttleBackoffSecs: 30, throttleBackoffCapSecs: 10 }")));
    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString("crawl.maxThrottledAttempts: 0")));
  }

  @Test
  public void testDefaults() {
    CrawlConfig crawlConfig = new CrawlConfig(ConfigFactory.empty());
    assertEquals("lucille_work", crawlConfig.workTopic);
    assertEquals("lucille_control", crawlConfig.controlTopic);
    assertEquals("lucille_crawlers", crawlConfig.consumerGroupId);
    assertEquals(16, crawlConfig.workTopicPartitions);
    assertEquals(1, crawlConfig.threads);
    assertEquals(64, crawlConfig.maxOutstandingUnits);
    assertEquals(3, crawlConfig.maxAttempts);
  }

  @Test
  public void testOverridesAndValidation() {
    CrawlConfig crawlConfig = new CrawlConfig(ConfigFactory.parseString("crawl { workTopic: work2, threads: 4, maxAttempts: 1 }"));
    assertEquals("work2", crawlConfig.workTopic);
    assertEquals(4, crawlConfig.threads);
    assertEquals(1, crawlConfig.maxAttempts);

    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString("crawl.threads: 0")));
    // a Crawler would treat every run as orphaned between heartbeats
    assertThrows(IllegalArgumentException.class,
        () -> new CrawlConfig(ConfigFactory.parseString("crawl { heartbeatSecs: 30, orphanTimeoutSecs: 30 }")));
  }

  @Test
  public void testNumbersMayBeSuppliedAsStrings() {
    // a value that comes from an environment variable substitution is a String, whatever it looks like
    CrawlConfig crawlConfig = new CrawlConfig(ConfigFactory.parseString(
        "crawl { threads: \"4\", workTopicPartitions: \"8\", maxOutstandingUnits: \"32\", maxAttempts: \"5\", "
            + "heartbeatSecs: \"2\", orphanTimeoutSecs: \"20\", topicReplicationFactor: \"3\" }"));

    assertEquals(4, crawlConfig.threads);
    assertEquals(8, crawlConfig.workTopicPartitions);
    assertEquals(32, crawlConfig.maxOutstandingUnits);
    assertEquals(5, crawlConfig.maxAttempts);
    assertEquals(2, crawlConfig.heartbeatSecs);
    assertEquals(20, crawlConfig.orphanTimeoutSecs);
    assertEquals(Short.valueOf((short) 3), crawlConfig.topicReplicationFactor);
  }

  @Test
  public void testTopicsFollowTheClusterUnlessToldOtherwise() {
    // with no replication factor configured, a topic is created with whatever the cluster's default is
    CrawlConfig byDefault = new CrawlConfig(ConfigFactory.empty());
    assertNull(byDefault.topicReplicationFactor);
    // -1 is how a NewTopic says that no replication factor was given
    assertEquals(-1, byDefault.newTopic("t", 4).replicationFactor());
    assertEquals(4, byDefault.newTopic("t", 4).numPartitions());

    CrawlConfig three = new CrawlConfig(ConfigFactory.parseString("crawl.topicReplicationFactor: 3"));
    assertEquals(3, three.newTopic("t", 1).replicationFactor());
    assertEquals(1, three.newTopic("t", 1).numPartitions());

    assertThrows(IllegalArgumentException.class,
        () -> new CrawlConfig(ConfigFactory.parseString("crawl.topicReplicationFactor: 0")));
  }

  @Test
  public void testSpecRejectsUnknownProperty() {
    Config config = ConfigFactory.parseString("workTopic: w, nonsense: 1");
    assertThrows(IllegalArgumentException.class, () -> CrawlConfig.SPEC.validate(config, "crawl"));
  }

  @Test
  public void testConnectorConfigLookup() {
    Config config = ConfigFactory.parseString(
        "connectors: [{class: a, pipeline: p}, {name: second, class: b}, {class: c}]");

    // unnamed connectors get the same default names as Connector.fromConfig() gives them
    assertEquals("a", CrawlConfig.connectorConfig(config, "connector_1").getString("class"));
    assertEquals("connector_1", CrawlConfig.connectorConfig(config, "connector_1").getString("name"));
    assertEquals("b", CrawlConfig.connectorConfig(config, "second").getString("class"));
    assertEquals("c", CrawlConfig.connectorConfig(config, "connector_3").getString("class"));
    assertNull(CrawlConfig.connectorConfig(config, "missing"));
  }

  @Test
  public void testConnectorConfigHash() {
    Config base = ConfigFactory.parseString("name: c1, class: a, paths: [\"s3://bucket/a\"], s3 { region: us-east-1 }");
    Config reordered = ConfigFactory.parseString("s3 { region: us-east-1 }, paths: [\"s3://bucket/a\"], class: a, name: c1");
    Config changed = ConfigFactory.parseString("name: c1, class: a, paths: [\"s3://bucket/b\"], s3 { region: us-east-1 }");

    assertEquals(CrawlConfig.connectorConfigHash(base), CrawlConfig.connectorConfigHash(reordered));
    assertNotEquals(CrawlConfig.connectorConfigHash(base), CrawlConfig.connectorConfigHash(changed));
  }

  @Test
  public void testConnectorConfigHashLeavesOutCredentials() {
    Config one = ConfigFactory.parseString(
        "name: c1, s3 { accessKeyId: one, secretAccessKey: one }, state { jdbcPassword: one, connectionString: one }, "
            + "apiKey: one, sasToken: one, authHeader: one");
    Config other = ConfigFactory.parseString(
        "name: c1, s3 { accessKeyId: other, secretAccessKey: other }, state { jdbcPassword: other, connectionString: other }, "
            + "apiKey: other, sasToken: other, authHeader: other");

    // Crawlers may be given different credentials than the Coordinator, and a hash that is sent over the network
    // should not let anyone confirm a guess at one
    assertEquals(CrawlConfig.connectorConfigHash(one), CrawlConfig.connectorConfigHash(other));
  }

  @Test
  public void testConnectorConfigHashDoesNotDependOnWhereTheConfigWasLoadedFrom() throws Exception {
    // the same connector, read from files with different names and with the connector on different lines
    File one = File.createTempFile("one", ".conf");
    File other = File.createTempFile("other", ".conf");
    String connector = "name: c1, class: a, paths: [\"s3://bucket/a\", \"s3://bucket/b\"], filterOptions { includes: [\"x\"] }";
    Files.writeString(one.toPath(), connector);
    Files.writeString(other.toPath(), "\n\n# moved down\n" + connector);

    assertEquals(CrawlConfig.connectorConfigHash(ConfigFactory.parseFile(one)),
        CrawlConfig.connectorConfigHash(ConfigFactory.parseFile(other)));
    one.delete();
    other.delete();
  }

  @Test
  public void testCrawlersNeedTheirOwnConsumerGroup() {
    assertThrows(IllegalArgumentException.class, () -> new CrawlConfig(ConfigFactory.parseString(
        "kafka.consumerGroupId: shared, crawl.consumerGroupId: shared")));
  }

  @Test
  public void testRunIdValidation() {
    assertTrue(CrawlConfig.isValidRunId("550e8400-e29b-41d4-a716-446655440000"));
    assertTrue(CrawlConfig.isValidRunId("Nightly_2026.10.04"));
    assertFalse(CrawlConfig.isValidRunId(null));
    assertFalse(CrawlConfig.isValidRunId(""));
    assertFalse(CrawlConfig.isValidRunId("a/b"));
    assertFalse(CrawlConfig.isValidRunId("a b"));
    assertFalse(CrawlConfig.isValidRunId("a\nb"));
    assertFalse(CrawlConfig.isValidRunId("x".repeat(129)));
  }

  @Test
  public void testRunConfigHashCoversEveryConnector() {
    Config config = ConfigFactory.parseString("connectors: [{name: a, class: x}, {name: b, class: y}]");
    Config changed = ConfigFactory.parseString("connectors: [{name: a, class: x}, {name: b, class: z}]");

    assertEquals(CrawlConfig.runConfigHash(config), CrawlConfig.runConfigHash(config));
    assertNotEquals(CrawlConfig.runConfigHash(config), CrawlConfig.runConfigHash(changed));
  }
}
