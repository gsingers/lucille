package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Files;
import org.junit.Test;

public class CrawlConfigTest {

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
