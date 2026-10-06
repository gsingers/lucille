package com.kmwllc.lucille.example.crawl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PartitionableConnector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.UnitContext;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.core.WorkUnitSink;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of a Connector written for a distributed crawl: it publishes one Document per file under a local
 * directory, and can split that work into units for many Crawlers to execute.
 *
 * <p>It is deliberately small, to show the mechanics a {@link PartitionableConnector} has to get right, in the order
 * they matter:
 * <ol>
 *   <li><b>Planning</b> ({@link #plan}): cut the source into units and name each with a key that is the same every
 *   time. Here: one unit for the files directly in the root, and one per directory beneath it.</li>
 *   <li><b>Executing a unit</b> ({@link #executeUnit}): read only the part of the source the unit describes, and
 *   publish Documents whose IDs do not depend on which unit, or which execution, found them.</li>
 *   <li><b>Checking what a unit asks for</b>: units arrive over the network, so a path is accepted only if it lies
 *   inside the configured root.</li>
 *   <li><b>Bounding a unit</b>: list at most {@code maxDirectoriesPerUnit} directories, and hand the ones found but
 *   not listed back through the {@link UnitContext}. The Coordinator makes units of them, so a subtree of any size is
 *   shared among Crawlers instead of being walked by one.</li>
 *   <li><b>Reporting and stopping</b>: count the calls made to the source, and stop when the unit has been given up
 *   on.</li>
 * </ol>
 *
 * <p>The same Connector also runs outside a distributed crawl: {@link #execute} walks the whole tree on one thread.
 *
 * <p>Config:
 * <ul>
 *   <li>root (String, Required): the directory to crawl.</li>
 *   <li>partitioning.maxDirectoriesPerUnit (Int, Optional): directories one unit lists before handing the rest back.
 *   Defaults to 10, small enough that a modest tree is shared out visibly.</li>
 * </ul>
 */
public class DirectoryTreeConnector extends AbstractConnector implements PartitionableConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("root")
      .optionalParent(SpecBuilder.parent("partitioning").optionalNumber("maxDirectoriesPerUnit").build())
      .build();

  // the fields of a unit's payload
  static final String PATH = "path";
  static final String RECURSIVE = "recursive";

  private static final Logger log = LoggerFactory.getLogger(DirectoryTreeConnector.class);

  private final Path root;
  private final int maxDirectoriesPerUnit;

  public DirectoryTreeConnector(Config config) throws ConnectorException {
    super(config);
    try {
      // the real path, so that a unit's path can be compared with it after its own links are resolved
      this.root = Paths.get(config.getString("root")).toRealPath();
    } catch (IOException e) {
      throw new ConnectorException("root " + config.getString("root") + " cannot be read.", e);
    }
    this.maxDirectoriesPerUnit = config.hasPath("partitioning.maxDirectoriesPerUnit")
        ? config.getInt("partitioning.maxDirectoriesPerUnit") : 10;
    if (maxDirectoriesPerUnit < 1) {
      throw new IllegalArgumentException("partitioning.maxDirectoriesPerUnit must be at least 1.");
    }
  }

  // ---- Outside a distributed crawl: the whole tree, on one thread ----

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    walk(publisher, root, true, Integer.MAX_VALUE, null);
  }

  // ---- 1. Planning ----

  /**
   * Called once per run, on the Coordinator. Emits one unit for the files directly in the root and one for each
   * directory beneath it. A Coordinator that resumes an interrupted run calls this again and skips the keys it
   * already has, so a key must name the same part of the source every time: here, the path relative to the root.
   */
  @Override
  public void plan(String runId, WorkUnitSink sink) throws ConnectorException {
    sink.emit("#files", unitPayload(root, false));

    for (Path directory : subdirectories(root)) {
      sink.emit(keyFor(directory), unitPayload(directory, true));
    }
  }

  // ---- 2 to 5. Executing a unit ----

  /**
   * Called on a Crawler, for one unit. May be called more than once for the same unit: if it fails, if its Crawler
   * dies, or if the work topic is rebalanced while it runs. Publishing the same Document IDs each time is what makes
   * that harmless.
   */
  @Override
  public void executeUnit(WorkUnit unit, Publisher publisher, UnitContext context) throws ConnectorException {
    Path start = checkedPath(unit);
    boolean recursive = unit.payload().path(RECURSIVE).asBoolean(true);

    List<Path> notListed = walk(publisher, start, recursive, maxDirectoriesPerUnit, context);

    // Directories found and not listed go back to the Coordinator, each to become a unit of its own. Their keys are
    // the keys the planner would give them, so a directory both planned and handed back is one unit, not two.
    for (Path directory : notListed) {
      context.handBack(keyFor(directory), unitPayload(directory, true));
    }
    if (!notListed.isEmpty()) {
      log.info("Unit {} handed back {} directories.", unit.unitId(), notListed.size());
    }
  }

  /**
   * Lists directories breadth first from the start, publishing the files in each, until it has listed the given
   * number. Returns the directories it found and did not list.
   *
   * @param context the unit's context, or null outside a distributed crawl.
   */
  private List<Path> walk(Publisher publisher, Path start, boolean recursive, int maxDirectories, UnitContext context)
      throws ConnectorException {
    Deque<Path> toList = new ArrayDeque<>();
    toList.add(start);
    int listed = 0;

    while (!toList.isEmpty()) {
      // 5. A unit that has timed out, lost its partition or lost its run is no longer wanted: stop, and leave what is
      // left. Nothing is handed back, since the unit will be executed again from the start.
      if (context != null && context.isCancelled()) {
        log.info("Unit starting at {} was given up on; stopping.", start);
        return List.of();
      }
      // 4. The budget is spent: what is still to be listed goes back. The first directory is always listed, so that
      // every unit makes progress.
      if (listed >= maxDirectories) {
        return new ArrayList<>(toList);
      }

      Path directory = toList.poll();
      List<Path> subdirectories = listAndPublish(publisher, directory);
      listed++;
      if (context != null) {
        context.addSourceCalls(1);
      }
      if (recursive) {
        toList.addAll(subdirectories);
      }
    }
    return List.of();
  }

  // Publishes a Document for each file directly in the directory, and returns its subdirectories.
  private List<Path> listAndPublish(Publisher publisher, Path directory) throws ConnectorException {
    List<Path> subdirectories = new ArrayList<>();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path entry : entries) {
        if (Files.isSymbolicLink(entry)) {
          // a link could lead outside the root, where no unit is allowed to go
          continue;
        }
        if (Files.isDirectory(entry)) {
          subdirectories.add(entry);
        } else if (Files.isRegularFile(entry)) {
          publisher.publish(toDocument(entry));
        }
      }
    } catch (ConnectorException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectorException("Could not list " + directory, e);
    }
    subdirectories.sort(null);
    return subdirectories;
  }

  // The ID is the path relative to the root: the same whichever unit, Crawler or execution finds the file.
  private Document toDocument(Path file) throws IOException {
    Document doc = Document.create(createDocId(keyFor(file)));
    doc.setField("file_path", file.toString());
    doc.setField("file_name", file.getFileName().toString());
    doc.setField("file_size_bytes", Files.size(file));
    return doc;
  }

  // ---- 3. Checking what a unit asks for ----

  /**
   * Returns the path a unit names, if it lies inside the root. A unit is a record on a Kafka topic, and a Crawler
   * must not read wherever a record tells it to. The real path is compared, so that a link cannot lead out.
   */
  private Path checkedPath(WorkUnit unit) throws ConnectorException {
    String path = unit.payload().path(PATH).asText(null);
    if (path == null) {
      throw new ConnectorException("Unit " + unit.unitId() + " names no path.");
    }
    try {
      Path real = Paths.get(path).toRealPath();
      if (!real.startsWith(root) || !Files.isDirectory(real)) {
        throw new ConnectorException("Unit " + unit.unitId() + " names a path outside " + root + ".");
      }
      return real;
    } catch (IOException e) {
      throw new ConnectorException("Unit " + unit.unitId() + " names a path that cannot be read: " + path, e);
    }
  }

  // ---- Helpers ----

  private ObjectNode unitPayload(Path directory, boolean recursive) {
    return WorkUnit.newPayload().put(PATH, directory.toString()).put(RECURSIVE, recursive);
  }

  private String keyFor(Path path) {
    String relative = root.relativize(path).toString().replace('\\', '/');
    return relative.isEmpty() ? "." : relative;
  }

  private List<Path> subdirectories(Path directory) throws ConnectorException {
    List<Path> subdirectories = new ArrayList<>();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, Files::isDirectory)) {
      for (Path entry : entries) {
        if (!Files.isSymbolicLink(entry)) {
          subdirectories.add(entry);
        }
      }
    } catch (IOException e) {
      throw new ConnectorException("Could not list " + directory, e);
    }
    subdirectories.sort(null);
    return subdirectories;
  }
}
