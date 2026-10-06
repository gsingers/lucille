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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import com.kmwllc.lucille.core.FailureClass;
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
        connector.executeUnit(unit(entry.getKey(), entry.getValue()), new PublisherImpl(config, messenger, "run1", "pipeline1"),
            new RecordingUnitContext());
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
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config("partitioning { depth: -1 }")));
    assertThrows(IllegalArgumentException.class,
        () -> new FileConnector(config("partitioning { maxDirectoriesPerUnit: 0 }")));
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config("partitioning { maxUnitSecs: 0 }")));
  }

  @Test
  public void testDepthMayBeSuppliedAsString() throws Exception {
    // as it is when it comes from an environment variable substitution
    Config config = config("partitioning { depth: \"2\" }");
    assertEquals(4, plan(config).size());
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config("partitioning { depth: \"-1\" }")));
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
  public void testDepthZeroPlansEachPathAsOneUnit() throws Exception {
    Config config = config("partitioning { depth: 0 }");
    Map<String, ObjectNode> units = plan(config);

    assertEquals(List.of(root.toUri().toString()), new ArrayList<>(units.keySet()));
    assertEquals(Set.of("top.txt", "a1.txt", "a2.txt", "b1.txt", "b2.txt"), allFiles(execute(config, units)));
  }

  /**
   * Executes the given units, then whatever they hand back, and so on until nothing is handed back. Returns the ID
   * of every Document published, with duplicates, and adds to the totals given.
   */
  private List<String> executeWithHandBack(Config config, Map<String, ObjectNode> units, int[] unitsExecuted, long[] sourceCalls)
      throws Exception {
    List<String> ids = new ArrayList<>();
    Map<String, ObjectNode> toExecute = new LinkedHashMap<>(units);
    Set<String> seen = new TreeSet<>(units.keySet());

    while (!toExecute.isEmpty()) {
      Map.Entry<String, ObjectNode> next = toExecute.entrySet().iterator().next();
      toExecute.remove(next.getKey());

      TestMessenger messenger = new TestMessenger();
      RecordingUnitContext context = new RecordingUnitContext();
      FileConnector connector = new FileConnector(config);
      connector.executeUnit(unit(next.getKey(), next.getValue()), new PublisherImpl(config, messenger, "run1", "pipeline1"), context);
      connector.close();

      messenger.getDocsSentForProcessing().forEach(doc -> ids.add(doc.getId()));
      unitsExecuted[0]++;
      sourceCalls[0] += context.sourceCalls;
      for (Map.Entry<String, ObjectNode> part : context.handedBack.entrySet()) {
        // no directory is handed back twice
        assertTrue(part.getKey(), seen.add(part.getKey()));
        toExecute.put(part.getKey(), part.getValue());
      }
    }
    return ids;
  }

  @Test
  public void testUnitHandsBackWhatItsBudgetDoesNotCover() throws Exception {
    // one directory per unit: the unit for root lists root, publishes its file, and hands back a/ and b/
    Config config = config("partitioning { depth: 0, maxDirectoriesPerUnit: 1 }");
    TestMessenger messenger = new TestMessenger();
    RecordingUnitContext context = new RecordingUnitContext();
    FileConnector connector = new FileConnector(config);
    Map<String, ObjectNode> units = plan(config);
    Map.Entry<String, ObjectNode> rootUnit = units.entrySet().iterator().next();

    connector.executeUnit(unit(rootUnit.getKey(), rootUnit.getValue()), new PublisherImpl(config, messenger, "run1", "pipeline1"), context);
    connector.close();

    assertEquals(Set.of("top.txt"), fileNames(messenger));
    assertEquals(List.of(root.resolve("a").toUri().toString(), root.resolve("b").toUri().toString()),
        new ArrayList<>(context.handedBack.keySet()));
    assertEquals(1, context.sourceCalls);
    // a handed-back directory is described as the planner would describe it, so it is executed the same way
    ObjectNode payload = context.handedBack.get(root.resolve("a").toUri().toString());
    assertEquals(root.resolve("a").toUri().toString(), payload.get("path").asText());
    assertTrue(payload.get("recursive").asBoolean());
  }

  @Test
  public void testHandBackPublishesEveryFileExactlyOnce() throws Exception {
    // the tree has four directories: root, a, a/deep, b
    TestMessenger wholeMessenger = new TestMessenger();
    Config unpartitioned = config("");
    FileConnector whole = new FileConnector(unpartitioned);
    whole.execute(new PublisherImpl(unpartitioned, wholeMessenger, "run1", "pipeline1"));
    whole.close();
    List<String> expectedIds = wholeMessenger.getDocsSentForProcessing().stream().map(Document::getId).sorted().toList();
    assertEquals(5, expectedIds.size());

    for (int depth = 0; depth <= 2; depth++) {
      for (int maxDirectories = 1; maxDirectories <= 5; maxDirectories++) {
        String label = "depth " + depth + ", maxDirectoriesPerUnit " + maxDirectories;
        Config config = config("partitioning { depth: " + depth + ", maxDirectoriesPerUnit: " + maxDirectories + " }");
        int[] unitsExecuted = {0};
        long[] sourceCalls = {0};

        List<String> ids = executeWithHandBack(config, plan(config), unitsExecuted, sourceCalls);

        // however the tree is cut, the same Documents come out as from a traversal that is not cut at all
        assertEquals(label, expectedIds, ids.stream().sorted().toList());
        // and no unit lists more directories than it is allowed
        assertTrue(label, sourceCalls[0] <= (long) unitsExecuted[0] * maxDirectories);
      }
    }

    // with no limit, nothing is handed back: the planned units are all there is
    Config unlimited = config("partitioning { depth: 0 }");
    int[] unitsExecuted = {0};
    long[] sourceCalls = {0};
    assertEquals(expectedIds, executeWithHandBack(unlimited, plan(unlimited), unitsExecuted, sourceCalls).stream().sorted().toList());
    assertEquals(1, unitsExecuted[0]);
    // the directories listed are counted even when they are not limited
    assertEquals(4, sourceCalls[0]);
  }

  @Test
  public void testDirectoriesAreHandedBackInGroupsThatFillAUnit() throws Exception {
    // six directories of six directories, two files in each: 43 directories and 86 files
    Path wide = Files.createTempDirectory("wide-tree");
    try {
      List<Path> level = List.of(wide);
      for (int depth = 0; depth < 3; depth++) {
        List<Path> next = new ArrayList<>();
        for (Path directory : level) {
          Files.writeString(directory.resolve("one.txt"), "one");
          Files.writeString(directory.resolve("two.txt"), "two");
          for (int i = 0; depth < 2 && i < 6; i++) {
            next.add(Files.createDirectory(directory.resolve("d" + i)));
          }
        }
        level = next;
      }

      Config config = ConfigFactory.parseString("""
          name: "files", class: "com.kmwllc.lucille.connector.FileConnector", pipeline: "pipeline1"
          paths: ["%s"]
          partitioning { depth: 0, maxDirectoriesPerUnit: 10 }
          """.formatted(wide.toUri()));
      int[] unitsExecuted = {0};
      long[] sourceCalls = {0};

      List<String> ids = executeWithHandBack(config, plan(config), unitsExecuted, sourceCalls);

      assertEquals(86, ids.size());
      assertEquals(86, new TreeSet<>(ids).size());
      assertEquals(43, sourceCalls[0]);
      // A unit that stops with directories left over hands them back together, as many to a unit as a unit may
      // list, and not one unit for each. So nearly every unit does a full unit's worth: 43 directories at 10 a unit
      // is 5 units at best, and it is 10 if every directory left over is a unit of its own.
      assertTrue("units executed: " + unitsExecuted[0], unitsExecuted[0] <= 7);
    } finally {
      try (Stream<Path> paths = Files.walk(wide)) {
        paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
      }
    }
  }

  @Test
  public void testHandBackGroupSizeCanDifferFromTheBudget() throws Exception {
    // one directory a unit, but handed back two to a unit: root lists root and hands back a/ and b/ together
    Config config = config("partitioning { depth: 0, maxDirectoriesPerUnit: 1, handBackGroupSize: 2 }");
    FileConnector connector = new FileConnector(config);
    RecordingUnitContext context = new RecordingUnitContext();
    Map<String, ObjectNode> units = plan(config);
    Map.Entry<String, ObjectNode> rootUnit = units.entrySet().iterator().next();

    connector.executeUnit(unit(rootUnit.getKey(), rootUnit.getValue()),
        new PublisherImpl(config, new TestMessenger(), "run1", "pipeline1"), context);
    connector.close();

    assertEquals(1, context.handedBack.size());
    ObjectNode payload = context.handedBack.values().iterator().next();
    assertEquals(2, payload.get("paths").size());
    assertEquals(root.resolve("a").toUri().toString(), payload.get("paths").get(0).asText());
    assertTrue(context.handedBack.keySet().iterator().next().startsWith(root.resolve("a").toUri() + "+1~"));

    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config("partitioning { handBackGroupSize: 0 }")));
  }

  @Test
  public void testUnitForSeveralDirectoriesIsHeldToTheConfiguredPaths() throws Exception {
    Config config = config("partitioning { depth: 0, maxDirectoriesPerUnit: 2 }");
    FileConnector connector = new FileConnector(config);
    TestMessenger messenger = new TestMessenger();

    // every directory a unit names is checked, not only the first
    ObjectNode payload = WorkUnit.newPayload().put("recursive", true);
    payload.putArray("paths").add(root.resolve("a").toUri().toString()).add(root.getParent().toUri().toString());
    assertThrows(ConnectorException.class, () -> connector.executeUnit(unit("group", payload),
        new PublisherImpl(config, messenger, "run1", "pipeline1"), new RecordingUnitContext()));
    assertTrue(messenger.getDocsSentForProcessing().isEmpty());

    ObjectNode empty = WorkUnit.newPayload().put("recursive", true);
    empty.putArray("paths");
    assertThrows(ConnectorException.class, () -> connector.executeUnit(unit("group", empty),
        new PublisherImpl(config, new TestMessenger(), "run1", "pipeline1"), new RecordingUnitContext()));

    // one that names two directories within the configured path is executed: both are walked, up to the limit
    ObjectNode both = WorkUnit.newPayload().put("recursive", true);
    both.putArray("paths").add(root.resolve("a").toUri().toString()).add(root.resolve("b").toUri().toString());
    RecordingUnitContext context = new RecordingUnitContext();
    TestMessenger bothMessenger = new TestMessenger();
    connector.executeUnit(unit("group", both), new PublisherImpl(config, bothMessenger, "run1", "pipeline1"), context);
    connector.close();

    // a/ and a/deep/ use up the two listings, so b/ is handed back
    assertEquals(Set.of("a1.txt", "a2.txt"), fileNames(bothMessenger));
    assertEquals(List.of(root.resolve("b").toUri().toString()), new ArrayList<>(context.handedBack.keySet()));
  }

  @Test
  public void testDirectoryThatCannotBeListedIsHandedBackAndEndsTheUnit() throws Exception {
    // b/ cannot be read; the walk reaches it after root and a/
    Path b = root.resolve("b");
    Assume.assumeTrue("needs a user that cannot read a directory it owns", b.toFile().setReadable(false, false)
        && !Files.isReadable(b));
    try {
      Config config = config("partitioning { depth: 0 }");
      FileConnector connector = new FileConnector(config);
      TestMessenger messenger = new TestMessenger();
      RecordingUnitContext context = new RecordingUnitContext();
      Map.Entry<String, ObjectNode> rootUnit = plan(config).entrySet().iterator().next();

      connector.executeUnit(unit(rootUnit.getKey(), rootUnit.getValue()), new PublisherImpl(config, messenger, "run1", "pipeline1"), context);
      connector.close();

      // what was listed was published, b/ was handed back to be tried as a unit of its own, and the unit completed
      assertEquals(Set.of("top.txt", "a1.txt", "a2.txt"), fileNames(messenger));
      assertEquals(List.of(b.toUri().toString()), new ArrayList<>(context.handedBack.keySet()));
      assertEquals(FailureClass.SOURCE_ERROR, context.errorClass);
      assertTrue(context.errorCause, context.errorCause.contains("AccessDenied"));

      // that unit meets the same failure, and hands back only itself
      FileConnector again = new FileConnector(config);
      RecordingUnitContext bContext = new RecordingUnitContext();
      again.executeUnit(unit(b.toUri().toString(), context.handedBack.get(b.toUri().toString())),
          new PublisherImpl(config, new TestMessenger(), "run1", "pipeline1"), bContext);
      again.close();
      assertEquals(List.of(b.toUri().toString()), new ArrayList<>(bContext.handedBack.keySet()));
      assertEquals(FailureClass.SOURCE_ERROR, bContext.errorClass);
    } finally {
      b.toFile().setReadable(true, true);
    }
  }

  @Test
  public void testSkippedDirectoryIsNotHandedBack() throws Exception {
    Config config = config("""
        partitioning { depth: 0, maxDirectoriesPerUnit: 1 }
        filterOptions { pathsToSkip: ["%s"] }
        """.formatted(root.resolve("b").toUri()));
    int[] unitsExecuted = {0};
    long[] sourceCalls = {0};

    List<String> ids = executeWithHandBack(config, plan(config), unitsExecuted, sourceCalls);

    // root, a and a/deep are each a unit; b is never one
    assertEquals(3, ids.size());
    assertEquals(3, unitsExecuted[0]);
  }

  @Test
  public void testTimeLimitHandsBackToo() throws Exception {
    // a limit of one second is not reached by a tree this small, so everything is walked by the one unit
    Config config = config("partitioning { depth: 0, maxUnitSecs: 1 }");
    int[] unitsExecuted = {0};
    long[] sourceCalls = {0};
    assertEquals(5, executeWithHandBack(config, plan(config), unitsExecuted, sourceCalls).size());
    assertEquals(1, unitsExecuted[0]);
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
        connector.executeUnit(unit(entry.getKey(), entry.getValue()), new PublisherImpl(config, unitMessenger, "run1", "pipeline1"),
            new RecordingUnitContext());
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
        // the same, with the dots written so that normalizing the URI does not remove them
        root.toUri() + "%2e%2e/outside/",
        // a different storage provider than the configured path
        "s3://bucket/key",
        "not a uri");

    for (String badPath : badPaths) {
      TestMessenger messenger = new TestMessenger();
      FileConnector connector = new FileConnector(config);
      WorkUnit unit = unit("bad", WorkUnit.newPayload().put("path", badPath).put("recursive", true));

      assertThrows(badPath, ConnectorException.class,
          () -> connector.executeUnit(unit, new PublisherImpl(config, messenger, "run1", "pipeline1"), new RecordingUnitContext()));
      assertTrue(badPath, messenger.getDocsSentForProcessing().isEmpty());
      connector.close();
    }
  }

  @Test
  public void testUnitUnderSkippedDirectoryIsRefused() throws Exception {
    Config config = config("""
        partitioning { depth: 1 }
        filterOptions { pathsToSkip: ["%s"] }
        """.formatted(root.resolve("a").toUri()));

    // the traversal turns back at a/, so nothing under it is ever planned, however deep
    for (Path skipped : List.of(root.resolve("a"), root.resolve("a/deep"), root.resolve("a/a1.txt"))) {
      TestMessenger messenger = new TestMessenger();
      FileConnector connector = new FileConnector(config);
      WorkUnit unit = unit("bad", WorkUnit.newPayload().put("path", skipped.toUri().toString()).put("recursive", true));

      assertThrows(skipped.toString(), ConnectorException.class,
          () -> connector.executeUnit(unit, new PublisherImpl(config, messenger, "run1", "pipeline1"), new RecordingUnitContext()));
      assertTrue(messenger.getDocsSentForProcessing().isEmpty());
      connector.close();
    }
  }

  @Test
  public void testUnitReachedThroughSymbolicLinkIsRefused() throws Exception {
    Config config = config("partitioning { depth: 1 }");
    Path outside = temp.newFolder("elsewhere").toPath().toRealPath();
    Files.createDirectories(outside.resolve("sub"));
    Files.writeString(outside.resolve("sub/secret.txt"), "not to be crawled");
    try {
      Files.createSymbolicLink(root.resolve("link"), outside);
    } catch (UnsupportedOperationException | IOException e) {
      Assume.assumeNoException("symbolic links are not available here", e);
    }

    // a traversal does not follow root/link, so the planner offers no unit for anything beyond it
    assertTrue(plan(config).keySet().stream().noneMatch(key -> key.contains("link")));

    // root/link/sub is inside the configured path as written, but not in fact
    for (String beyondLink : List.of("link/sub", "link/sub/secret.txt", "link")) {
      TestMessenger messenger = new TestMessenger();
      FileConnector connector = new FileConnector(config);
      WorkUnit unit = unit("bad", WorkUnit.newPayload().put("path", root.toUri() + beyondLink).put("recursive", true));

      assertThrows(beyondLink, ConnectorException.class,
          () -> connector.executeUnit(unit, new PublisherImpl(config, messenger, "run1", "pipeline1"), new RecordingUnitContext()));
      assertTrue(messenger.getDocsSentForProcessing().isEmpty());
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
