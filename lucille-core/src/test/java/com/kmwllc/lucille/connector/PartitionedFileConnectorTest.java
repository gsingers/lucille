package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests FileConnector as a PartitionableConnector: how it splits its paths into work units, and what executing
 * those units publishes. Each test plans on one instance and executes on others, as a distributed crawl would.
 */
public class PartitionedFileConnectorTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Path root;

  // root/top.txt, root/a/a1.txt, root/a/deep/a2.txt, root/b/b1.txt, root/b/b2.txt
  @Before
  public void setUp() throws Exception {
    root = temp.newFolder("root").toPath().toRealPath();
    write("top.txt");
    write("a/a1.txt");
    write("a/deep/a2.txt");
    write("b/b1.txt");
    write("b/b2.txt");
  }

  private void write(String relativePath) throws Exception {
    Path file = root.resolve(relativePath);
    Files.createDirectories(file.getParent());
    Files.writeString(file, "contents of " + relativePath);
  }

  private Config config(String extra) {
    // the path is written as a URI so that it is a valid config value on every platform
    return ConfigFactory.parseString("""
        name: "files", class: "com.kmwllc.lucille.connector.FileConnector", pipeline: "pipeline1"
        paths: ["%s"]
        %s
        """.formatted(root.toUri(), extra));
  }

  /** Plans with a fresh connector and returns the payload of each unit, by unit key, in the order emitted. */
  private Map<String, ObjectNode> plan(Config config) throws Exception {
    Map<String, ObjectNode> units = new LinkedHashMap<>();
    FileConnector connector = new FileConnector(config);
    try {
      connector.plan("run1", units::put);
    } finally {
      connector.close();
    }
    return units;
  }

  private static WorkUnit unit(String key, ObjectNode payload) {
    return new WorkUnit("run1", "files", "pipeline1", "files/" + key, 1, 1, "hash", payload);
  }

  /** Executes each unit on a connector of its own and returns the names of the files published, per unit. */
  private Map<String, Set<String>> execute(Config config, Map<String, ObjectNode> units) throws Exception {
    Map<String, Set<String>> published = new LinkedHashMap<>();

    for (Map.Entry<String, ObjectNode> entry : units.entrySet()) {
      TestMessenger messenger = new TestMessenger();
      FileConnector connector = new FileConnector(config);
      try {
        connector.executeUnit(unit(entry.getKey(), entry.getValue()), new PublisherImpl(config, messenger, "run1", "pipeline1"));
      } finally {
        connector.close();
      }
      published.put(entry.getKey(), fileNames(messenger));
    }

    return published;
  }

  private static Set<String> fileNames(TestMessenger messenger) {
    return messenger.getDocsSentForProcessing().stream()
        .map(doc -> new File(doc.getString(FileConnector.FILE_PATH)).getName())
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static Set<String> allFiles(Map<String, Set<String>> published) {
    return published.values().stream().flatMap(Set::stream).collect(Collectors.toCollection(TreeSet::new));
  }

  @Test
  public void testPartitioningIsOptIn() throws Exception {
    assertFalse(new FileConnector(config("")).isPartitioningEnabled());
    assertTrue(new FileConnector(config("partitioning {}")).isPartitioningEnabled());
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config("partitioning { depth: 0 }")));
  }

  @Test
  public void testPlanSplitsByDirectory() throws Exception {
    Config config = config("partitioning { depth: 1 }");
    Map<String, ObjectNode> units = plan(config);

    // one unit for the files directly under the path, and one for each directory under it
    assertEquals(3, units.size());
    List<String> keys = new ArrayList<>(units.keySet());
    assertEquals(root.toUri() + "#files", keys.get(0));
    assertFalse(units.get(keys.get(0)).get("recursive").asBoolean());
    assertEquals(root.resolve("a").toUri().toString(), keys.get(1));
    assertTrue(units.get(keys.get(1)).get("recursive").asBoolean());
    assertEquals(root.resolve("b").toUri().toString(), keys.get(2));

    Map<String, Set<String>> published = execute(config, units);
    assertEquals(Set.of("top.txt"), published.get(keys.get(0)));
    assertEquals(Set.of("a1.txt", "a2.txt"), published.get(keys.get(1)));
    assertEquals(Set.of("b1.txt", "b2.txt"), published.get(keys.get(2)));
  }

  @Test
  public void testPlanIsStable() throws Exception {
    Config config = config("partitioning { depth: 2 }");
    assertEquals(new ArrayList<>(plan(config).keySet()), new ArrayList<>(plan(config).keySet()));
  }

  @Test
  public void testUnitsPublishTheSameDocumentsAsExecute() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config unpartitioned = config("");
    FileConnector whole = new FileConnector(unpartitioned);
    whole.execute(new PublisherImpl(unpartitioned, messenger, "run1", "pipeline1"));
    whole.close();
    Set<String> expectedIds = messenger.getDocsSentForProcessing().stream().map(Document::getId).collect(Collectors.toSet());
    assertEquals(5, expectedIds.size());

    for (int depth = 1; depth <= 3; depth++) {
      Config config = config("partitioning { depth: " + depth + " }");
      Set<String> ids = new TreeSet<>();
      int numDocs = 0;

      for (Map.Entry<String, ObjectNode> entry : plan(config).entrySet()) {
        TestMessenger unitMessenger = new TestMessenger();
        FileConnector connector = new FileConnector(config);
        connector.executeUnit(unit(entry.getKey(), entry.getValue()), new PublisherImpl(config, unitMessenger, "run1", "pipeline1"));
        connector.close();
        numDocs += unitMessenger.getDocsSentForProcessing().size();
        unitMessenger.getDocsSentForProcessing().forEach(doc -> ids.add(doc.getId()));
      }

      // every file is published by exactly one unit, under the ID it would have had without partitioning
      assertEquals("depth " + depth, 5, numDocs);
      assertEquals("depth " + depth, expectedIds, ids);
    }
  }

  @Test
  public void testDeeperPlan() throws Exception {
    Config config = config("partitioning { depth: 2 }");
    Map<String, ObjectNode> units = plan(config);

    // root#files, a#files, a/deep, b#files
    assertEquals(4, units.size());
    assertTrue(units.containsKey(root.resolve("a").toUri() + "#files"));
    assertTrue(units.containsKey(root.resolve("a/deep").toUri().toString()));
    assertEquals(Set.of("a1.txt"), execute(config, units).get(root.resolve("a").toUri() + "#files"));
  }

  @Test
  public void testSkippedDirectoryIsNotPlanned() throws Exception {
    Config config = config("""
        partitioning { depth: 1 }
        filterOptions { pathsToSkip: ["%s"] }
        """.formatted(root.resolve("b").toUri()));
    Map<String, ObjectNode> units = plan(config);

    assertEquals(2, units.size());
    assertEquals(Set.of("top.txt", "a1.txt", "a2.txt"), allFiles(execute(config, units)));
  }

  @Test
  public void testPathToSingleFile() throws Exception {
    Config config = ConfigFactory.parseString("""
        name: "files", class: "com.kmwllc.lucille.connector.FileConnector", pipeline: "pipeline1"
        paths: ["%s"]
        partitioning { depth: 1 }
        """.formatted(root.resolve("top.txt").toUri()));
    Map<String, ObjectNode> units = plan(config);

    assertEquals(1, units.size());
    assertEquals(Set.of("top.txt"), allFiles(execute(config, units)));
  }

  @Test
  public void testUnitOutsideConfiguredPathsIsRefused() throws Exception {
    Config config = config("partitioning { depth: 1 }");
    Path outside = temp.newFolder("outside").toPath().toRealPath();
    Files.writeString(outside.resolve("secret.txt"), "not to be crawled");

    List<String> badPaths = List.of(
        outside.toUri().toString(),
        // a path that begins inside the configured path and climbs out of it
        root.toUri() + "../outside/",
        // a different storage provider than the configured path
        "s3://bucket/key",
        "not a uri");

    for (String badPath : badPaths) {
      TestMessenger messenger = new TestMessenger();
      FileConnector connector = new FileConnector(config);
      WorkUnit unit = unit("bad", WorkUnit.newPayload().put("path", badPath).put("recursive", true));

      assertThrows(badPath, ConnectorException.class,
          () -> connector.executeUnit(unit, new PublisherImpl(config, messenger, "run1", "pipeline1")));
      assertTrue(badPath, messenger.getDocsSentForProcessing().isEmpty());
      connector.close();
    }
  }

  @Test
  public void testStateRequiresSharedDatabase() throws Exception {
    FileConnector connector = new FileConnector(config("""
        partitioning { depth: 1 }
        state { enabled: true }
        """));

    // the default is a database embedded in the working directory, which other Crawlers could not reach
    ConnectorException e = assertThrows(ConnectorException.class, () -> connector.prepareRun("run1"));
    assertTrue(e.getMessage().contains("state.connectionString"));
  }

  @Test
  public void testTombstonesArePublishedOnceAfterAllUnits() throws Exception {
    Config config = config("""
        partitioning { depth: 1 }
        filterOptions { publishMode: "incremental", sendTombstones: true }
        state { connectionString: "jdbc:h2:mem:partitionedTombstones;DB_CLOSE_DELAY=-1" }
        """);

    // first run: every file is published and recorded, and nothing has expired
    assertEquals(5, runWithState(config, "run1").size());
    assertTrue(finalizeRun(config, "run1").isEmpty());

    // second run: one file is gone. No unit can tell, since each sees only its own directory.
    Files.delete(root.resolve("b/b2.txt"));
    assertTrue(runWithState(config, "run2").isEmpty());

    List<Document> tombstones = finalizeRun(config, "run2");
    assertEquals(1, tombstones.size());
    assertTrue(tombstones.get(0).getString(FileConnector.FILE_PATH).endsWith("b2.txt"));
    assertTrue(tombstones.get(0).getBoolean(FileConnector.EXPIRED));
  }

  // The Coordinator's part of a run before the units, prepareRun() and plan(), and then the units, each on a connector
  // of its own. The Coordinator's connector is never closed, as that of a Coordinator that stopped part way would not be.
  private Set<String> runWithState(Config config, String runId) throws Exception {
    FileConnector coordinator = new FileConnector(config);
    coordinator.prepareRun(runId);
    Map<String, ObjectNode> units = new LinkedHashMap<>();
    coordinator.plan(runId, units::put);
    return allFiles(execute(config, units));
  }

  // The Coordinator's part of a run after the units, on a new connector, as a Coordinator that resumed the run would have.
  private List<Document> finalizeRun(Config config, String runId) throws Exception {
    TestMessenger messenger = new TestMessenger();
    FileConnector coordinator = new FileConnector(config);
    try {
      coordinator.finalizeRun(runId, new PublisherImpl(config, messenger, runId, "pipeline1"));
    } finally {
      coordinator.close();
    }
    return messenger.getDocsSentForProcessing();
  }
}
