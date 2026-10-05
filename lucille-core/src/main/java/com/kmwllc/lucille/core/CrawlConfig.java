package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Settings for a distributed crawl, read from the optional <code>crawl</code> block of a Lucille config, along with
 * helpers for the parts of the config that the Coordinator and Crawlers have to agree on.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>workTopic (String, Optional) : Kafka topic that work units are dispatched to. Shared by all runs. Defaults to "lucille_work".</li>
 *   <li>workTopicPartitions (Int, Optional) : Partitions to create the work topic with, if it does not exist. At most this
 *   many units are executed at once across all Crawlers. Defaults to 16.</li>
 *   <li>controlTopic (String, Optional) : Compacted Kafka topic carrying each run's heartbeat and cancellation. Defaults to "lucille_control".</li>
 *   <li>topicReplicationFactor (Int, Optional) : Replication factor used when creating the work topic, the control topic and each
 *   run's event topic. Defaults to the Kafka cluster's own default (<code>default.replication.factor</code>).</li>
 *   <li>consumerGroupId (String, Optional) : Consumer group for Crawlers. Must differ from kafka.consumerGroupId. Defaults to "lucille_crawlers".</li>
 *   <li>threads (Int, Optional) : Units executed concurrently by one Crawler process. Defaults to 1.</li>
 *   <li>maxOutstandingUnits (Int, Optional) : The Coordinator dispatches no more units while this many are incomplete. Defaults to 64.</li>
 *   <li>maxAttempts (Int, Optional) : Times a unit is dispatched before its failure fails the connector. Defaults to 3.</li>
 *   <li>heartbeatSecs (Int, Optional) : Period of the Coordinator's heartbeat. Defaults to 10.</li>
 *   <li>orphanTimeoutSecs (Int, Optional) : Crawlers give up on a run whose heartbeat is older than this. Defaults to 120.</li>
 * </ul>
 */
public final class CrawlConfig {

  public static final Spec SPEC = SpecBuilder.withoutDefaults()
      .optionalString("workTopic", "controlTopic", "consumerGroupId")
      .optionalNumber("workTopicPartitions", "topicReplicationFactor", "threads", "maxOutstandingUnits", "maxAttempts",
          "heartbeatSecs", "orphanTimeoutSecs").build();

  // Config keys whose values are left out of the config hash, so that a hash published to Kafka cannot be used to
  // confirm a guess at a credential.
  private static final Pattern SECRET_KEY = Pattern.compile(
      "(?i).*(password|passphrase|pwd|secret|token|key|credential|auth|jaas|sas|connectionstring).*");

  // A run ID becomes part of a Kafka topic name, which allows only these characters and at most 249 of them in all.
  private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Returns whether the given run ID can be used for a distributed crawl.
   */
  public static boolean isValidRunId(String runId) {
    return runId != null && RUN_ID.matcher(runId).matches();
  }

  public final String workTopic;
  public final int workTopicPartitions;
  public final String controlTopic;
  // null when not configured, in which case topics are created with the cluster's default
  public final Short topicReplicationFactor;
  public final String consumerGroupId;
  public final int threads;
  public final int maxOutstandingUnits;
  public final int maxAttempts;
  public final int heartbeatSecs;
  public final int orphanTimeoutSecs;

  public CrawlConfig(Config config) {
    this.workTopic = ConfigUtils.getOrDefault(config, "crawl.workTopic", "lucille_work");
    this.workTopicPartitions = atLeastOne(config, "crawl.workTopicPartitions", 16);
    this.controlTopic = ConfigUtils.getOrDefault(config, "crawl.controlTopic", "lucille_control");
    this.topicReplicationFactor = config.hasPath("crawl.topicReplicationFactor")
        ? (short) atLeastOne(config, "crawl.topicReplicationFactor", 1) : null;
    this.consumerGroupId = ConfigUtils.getOrDefault(config, "crawl.consumerGroupId", "lucille_crawlers");
    this.threads = atLeastOne(config, "crawl.threads", 1);
    this.maxOutstandingUnits = atLeastOne(config, "crawl.maxOutstandingUnits", 64);
    this.maxAttempts = atLeastOne(config, "crawl.maxAttempts", 3);
    this.heartbeatSecs = atLeastOne(config, "crawl.heartbeatSecs", 10);
    this.orphanTimeoutSecs = atLeastOne(config, "crawl.orphanTimeoutSecs", 120);

    if (orphanTimeoutSecs <= heartbeatSecs) {
      throw new IllegalArgumentException("crawl.orphanTimeoutSecs must be greater than crawl.heartbeatSecs.");
    }

    // In one group, every Crawler that joined or left would interrupt the Workers, and the reverse.
    if (config.hasPath("kafka.consumerGroupId") && consumerGroupId.equals(config.getString("kafka.consumerGroupId"))) {
      throw new IllegalArgumentException("crawl.consumerGroupId must differ from kafka.consumerGroupId.");
    }
  }

  private static int atLeastOne(Config config, String path, int defaultValue) {
    // getInt accepts a number written as a String, which is what a value substituted from the environment is
    int value = config.hasPath(path) ? config.getInt(path) : defaultValue;
    if (value < 1) {
      throw new IllegalArgumentException(path + " must be at least 1.");
    }
    return value;
  }

  /**
   * Describes a topic to be created for a distributed crawl. The work topic, the control topic and a run's event
   * topic all hold what a run needs in order to continue or be resumed, so they share one replication setting.
   */
  public NewTopic newTopic(String name, int numPartitions) {
    return new NewTopic(name, Optional.of(numPartitions), Optional.ofNullable(topicReplicationFactor));
  }

  /**
   * Returns the config of the connector with the given name, applying the same default names as
   * {@link Connector#fromConfig(Config)}, or null if no connector has that name.
   */
  public static Config connectorConfig(Config config, String connectorName) {
    List<? extends Config> connectorConfigs = config.getConfigList("connectors");

    for (int i = 0; i < connectorConfigs.size(); i++) {
      Config connectorConfig = connectorConfigs.get(i);
      String name = connectorConfig.hasPath("name") ? connectorConfig.getString("name") : ("connector_" + (i + 1));

      if (name.equals(connectorName)) {
        return connectorConfig.withValue("name", ConfigValueFactory.fromAnyRef(name));
      }
    }

    return null;
  }

  /**
   * Hashes a connector's config, so that the Coordinator and a Crawler can tell whether they were given the same one.
   * Values that look like credentials are left out.
   */
  public static String connectorConfigHash(Config connectorConfig) {
    Map<String, String> entries = new TreeMap<>();

    for (Map.Entry<String, ConfigValue> entry : connectorConfig.entrySet()) {
      if (!SECRET_KEY.matcher(entry.getKey()).matches()) {
        // rendered concisely, as the default rendering includes where in which file each value came from
        entries.put(entry.getKey(), entry.getValue().render(ConfigRenderOptions.concise()));
      }
    }

    return sha256(entries.toString());
  }

  /**
   * Hashes the configs of all connectors, in order, to identify the config a run was started with.
   */
  public static String runConfigHash(Config config) {
    StringBuilder hashes = new StringBuilder();
    for (Config connectorConfig : config.getConfigList("connectors")) {
      hashes.append(connectorConfigHash(connectorConfig)).append('\n');
    }
    return sha256(hashes.toString());
  }

  private static String sha256(String s) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(s.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Builds the message carried by a unit lifecycle or control Event. */
  public static ObjectNode newMessage() {
    return MAPPER.createObjectNode();
  }

  /** Parses the message carried by a unit lifecycle or control Event. */
  public static ObjectNode parseMessage(String message) throws Exception {
    return (ObjectNode) MAPPER.readTree(message);
  }
}
