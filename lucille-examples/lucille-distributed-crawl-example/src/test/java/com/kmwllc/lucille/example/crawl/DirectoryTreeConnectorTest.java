package com.kmwllc.lucille.example.crawl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.UnitContext;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the example the way a Connector author should test their own: call plan() and executeUnit() directly, play
 * the Coordinator's part (turn handed-back parts into units, until there are none), and check that every file is
 * published exactly once however the tree is cut. Then run the whole crawl in process.
 */
public class DirectoryTreeConnectorTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Path root;

  @Before
  public void setUp() throws Exception {
    root = temp.newFolder("tree").toPath();
    SampleTree.write(root);
  }

  private Config config(int maxDirectoriesPerUnit) {
    return ConfigFactory.parseString("""
        name: "tree", pipeline: "crawl_pipeline", partitioning.maxDirectoriesPerUnit: %d
        """.formatted(maxDirectoriesPerUnit)).withValue("root", ConfigValueFactory.fromAnyRef(root.toString()));
  }

  private static WorkUnit unit(String key, ObjectNode payload) {
    return new WorkUnit("run1", "tree", "crawl_pipeline", "tree/" + key, 1, 1, "hash", payload);
  }

  /** A UnitContext that keeps what a unit hands back. */
  private static class Recorder implements UnitContext {
    final Map<String, ObjectNode> handedBack = new LinkedHashMap<>();
    long sourceCalls = 0;

    @Override
    public void handBack(String unitKey, ObjectNode payload) {
      handedBack.put(unitKey, payload);
    }

    @Override
    public void addSourceCalls(long calls) {
      sourceCalls += calls;
    }
  }

  /** Plans, then executes units and what they hand back until nothing is left; returns the Document IDs published. */
  private List<String> crawl(Config config, int[] unitsExecuted) throws Exception {
    DirectoryTreeConnector connector = new DirectoryTreeConnector(config);
    Map<String, ObjectNode> planned = new LinkedHashMap<>();
    connector.plan("run1", planned::put);

    Deque<Map.Entry<String, ObjectNode>> toExecute = new ArrayDeque<>(planned.entrySet());
    Set<String> known = new HashSet<>(planned.keySet());
    List<String> ids = new ArrayList<>();

    while (!toExecute.isEmpty()) {
      Map.Entry<String, ObjectNode> next = toExecute.poll();
      TestMessenger messenger = new TestMessenger();
      Recorder context = new Recorder();
      connector.executeUnit(unit(next.getKey(), next.getValue()), new PublisherImpl(config, messenger, "run1", "crawl_pipeline"), context);

      messenger.getDocsSentForProcessing().stream().map(Document::getId).forEach(ids::add);
      unitsExecuted[0]++;
      // the Coordinator's rule: a part that is already a unit is not created twice
      for (Map.Entry<String, ObjectNode> part : context.handedBack.entrySet()) {
        if (known.add(part.getKey())) {
          toExecute.add(part);
        }
      }
    }
    return ids;
  }

  @Test
  public void testPlanIsOneUnitForTheRootsFilesAndOnePerDirectoryBeneathIt() throws Exception {
    Map<String, ObjectNode> planned = new LinkedHashMap<>();
    new DirectoryTreeConnector(config(10)).plan("run1", planned::put);

    assertEquals(List.of("#files", "big", "small0", "small1", "small2", "small3", "small4"), new ArrayList<>(planned.keySet()));
    assertFalse(planned.get("#files").get("recursive").asBoolean());
    assertTrue(planned.get("big").get("recursive").asBoolean());
  }

  @Test
  public void testEveryFileIsPublishedExactlyOnceHoweverTheTreeIsCut() throws Exception {
    for (int limit : new int[] {1, 3, 10, 1000}) {
      int[] units = {0};
      List<String> ids = crawl(config(limit), units);

      assertEquals("limit " + limit, SampleTree.NUM_FILES, ids.size());
      assertEquals("limit " + limit, SampleTree.NUM_FILES, new HashSet<>(ids).size());
    }
  }

  @Test
  public void testTheLargeSubtreeIsSharedOut() throws Exception {
    // with no limit, the planned units are all there is: big/ is one unit, walked alone
    int[] unbounded = {0};
    crawl(config(1000), unbounded);
    assertEquals(7, unbounded[0]);

    // with a limit of 10 directories, big/ (111 directories) is handed back in pieces
    int[] bounded = {0};
    crawl(config(10), bounded);
    assertTrue("units: " + bounded[0], bounded[0] > 7 + 10);
  }

  @Test
  public void testAUnitOutsideTheRootIsRefused() throws Exception {
    DirectoryTreeConnector connector = new DirectoryTreeConnector(config(10));
    Path outside = temp.newFolder("elsewhere").toPath();
    Files.writeString(outside.resolve("secret.txt"), "not yours");

    for (String path : List.of(outside.toString(), root.resolve("..").resolve("elsewhere").toString(), "/no/such/dir")) {
      ObjectNode payload = WorkUnit.newPayload().put("path", path).put("recursive", true);
      assertThrows(ConnectorException.class, () -> connector.executeUnit(unit("x", payload),
          new PublisherImpl(config(10), new TestMessenger(), "run1", "crawl_pipeline"), new Recorder()));
    }
  }

  @Test
  public void testAUnitThatIsGivenUpOnStopsAndHandsNothingBack() throws Exception {
    DirectoryTreeConnector connector = new DirectoryTreeConnector(config(1000));
    TestMessenger messenger = new TestMessenger();
    Recorder cancelled = new Recorder() {
      @Override
      public boolean isCancelled() {
        return true;
      }
    };

    connector.executeUnit(unit("big", WorkUnit.newPayload().put("path", root.resolve("big").toString()).put("recursive", true)),
        new PublisherImpl(config(1000), messenger, "run1", "crawl_pipeline"), cancelled);

    assertTrue(messenger.getDocsSentForProcessing().isEmpty());
    assertTrue(cancelled.handedBack.isEmpty());
  }

  @Test
  public void testTheWholeCrawlRunsInProcess() throws Exception {
    Path output = temp.getRoot().toPath().resolve("crawl-output.csv");
    // the example's own config, with the tree and the output moved to temporary places
    Config config = ConfigFactory.parseFile(new java.io.File("conf/crawl.conf"))
        .withValue("connectors", ConfigValueFactory.fromIterable(List.of(Map.of(
            "name", "tree", "class", DirectoryTreeConnector.class.getName(), "pipeline", "crawl_pipeline",
            "root", root.toString(), "partitioning", Map.of("maxDirectoriesPerUnit", 10)))))
        .withValue("csv.path", ConfigValueFactory.fromAnyRef(output.toString()))
        .resolve();

    RunResult result = InProcessDemo.run(config, "test-run");

    assertTrue(result.toString(), result.getStatus());
    // a header and one row per file
    assertEquals(SampleTree.NUM_FILES + 1, Files.readAllLines(output).size());
  }
}
