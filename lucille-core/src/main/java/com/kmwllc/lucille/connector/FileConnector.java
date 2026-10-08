package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.util.ThreadNameUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.storageclient.BaseFileReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import com.kmwllc.lucille.connector.storageclient.SourceRetryPolicy;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalBudget;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.connector.storageclient.TraversalParams.PublishMode;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.FailureClass;
import com.kmwllc.lucille.core.PartitionableConnector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.UnitContext;
import com.kmwllc.lucille.core.WorkUnit;
import com.kmwllc.lucille.core.WorkUnitSink;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Traverses local and cloud storage (S3, GCP, Azure) from one or more roots and publishes a Document for each file encountered.
 * Supports include/exclude regex filters, recency cutoffs, optional content fetching, archive/compressed file handling, file moves
 * after processing or on error, and optional JDBC-backed state to avoid republishing recently handled files. Only files matching
 * all filter criteria are processed. Durations use HOCON-style strings like "1h", "2d", "3s".
 *
 * For archive/compressed files, modification/publish cutoffs apply to both the container and its entries. When state is enabled,
 * traversal may be slower. Files that are moved/renamed are always republished regardless of lastPublishedCutoff. You can enable
 * state without specifying lastPublishedCutoff to keep publish times updated.
 *
 * State tracks file paths and last publish timestamps to support filterOptions.lastPublishedCutoff and to avoid duplicate publications
 * across runs. You can connect to your own JDBC database by providing driver, connectionString, jdbcUser, and tableName. If
 * connectionString is omitted, an embedded database is created at ./state/{CONNECTOR_NAME}. With state enabled, traversal may be
 * slower, and files that are moved or renamed are always republished. The lastPublishedCutoff setting has no effect unless state is
 * configured. You may enable state without lastPublishCutoff and publish times will still be updated.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>paths (List&lt;String&gt;, Required) : Paths or URIs to traverse (local paths or cloud storage URIs). s3 URIs must be
 *   percent-encoded; unencoded spaces or special characters will not be recognized. For example, use s3://test/folder%20with%20spaces.</li>
 *   <li>concurrent (Boolean, Optional) : Traverse each of the paths concurrently, on its own thread. Only applies
 *   when more than one path is given. Requires that the paths do not overlap. Defaults to false.</li>
 *   <li>filterOptions.includes (List&lt;String&gt;, Optional) : Regex patterns to include files.</li>
 *   <li>filterOptions.excludes (List&lt;String&gt;, Optional) : Regex patterns to exclude files.</li>
 *   <li>filterOptions.pathsToSkip (List&lt;String&gt;, Optional) : URIs of paths to directories that should be skipped and not traversed.</li>
 *   <li>filterOptions.lastModifiedCutoff (String, Optional) : Duration string to include only files modified within this period (e.g., "1h").</li>
 *   <li>filterOptions.lastPublishedCutoff (String, Optional) : Duration string to include only files not published within this period.</li>
 *   <li>filterOptions.publishMode (String, Optional) : Set as 'incremental' or 'full' to choose mode of publishing.</li>
 *   <li>fileOptions.getFileContent (Boolean, Optional) : Fetch file content during traversal. Defaults to true.</li>
 *   <li>fileOptions.handleArchivedFiles (Boolean, Optional) : Process archive files. Defaults to false.</li>
 *   <li>fileOptions.handleCompressedFiles (Boolean, Optional) : Process compressed files. Defaults to false.</li>
 *   <li>fileOptions.moveToAfterProcessing (String, Optional) : URI to move files after successful processing (single input path only).</li>
 *   <li>fileOptions.moveToErrorFolder (String, Optional) : URI to move files if processing fails (single input path only).</li>
 *   <li>state.enabled (Boolean, Optional) : Set state database to be enabled or disabled (null). Defaults to true.</li>
 *   <li>state.driver (String, Optional) : JDBC driver class. Defaults to "org.h2.Driver".</li>
 *   <li>state.connectionString (String, Optional) : JDBC connection string. Defaults to "jdbc:h2:./state/{CONNECTOR_NAME}".</li>
 *   <li>state.jdbcUser (String, Optional) : Database username. Defaults to "".</li>
 *   <li>state.jdbcPassword (String, Optional) : Database password. Defaults to "".</li>
 *   <li>state.tableName (String, Optional) : Table name for state. Defaults to the connector name.</li>
 *   <li>state.performDeletions (Boolean, Optional) : Delete rows for files removed from storage. Defaults to true.</li>
 *   <li>state.runsBeforeExpiration (Int, Optional) : After a file is not encountered for this number of runs, it will be marked
 *   as expired. Must be at least 1. Defaults to 1.</li>
 *   <li>state.pathLength (Int, Optional) : Max length for stored file paths when Lucille creates the table. Defaults to 200.</li>
 *   <li>partitioning.depth (Int, Optional) : In a distributed crawl, split each path into one work unit per directory this
 *   many levels below it, plus one for the files directly in each directory above that level. With 0, each path is one
 *   unit. Local paths and S3 are split; paths in other providers become one unit each. Defaults to 1. Without a
 *   partitioning block, a distributed crawl traverses all paths as a single unit. A partitioned connector that uses
 *   state needs state.connectionString to name a database every Crawler can reach, and applies expiry and
 *   sendTombstones once, after all units are done.</li>
 *   <li>partitioning.maxDirectoriesPerUnit (Int, Optional) : In a distributed crawl, the number of directories one work
 *   unit lists before it stops descending. The directories it has found and not listed are handed back, in groups
 *   of this many, and each group becomes a unit, so that a large subtree is shared among Crawlers instead of being
 *   walked by one.
 *   Local paths and S3 only. Not set by default, so a unit walks all of its subtree.</li>
 *   <li>partitioning.maxUnitSecs (Int, Optional) : As maxDirectoriesPerUnit, but a limit on how long a unit goes on
 *   listing directories. Either limit, when reached, ends the unit's descent.</li>
 *   <li>sourceRetrySecs (Int, Optional) : How long, in all, to go on retrying a call that the source refused for
 *   rate (HTTP 429) or could not answer (5xx, connection failures), with exponential backoff. Defaults to 180. In a
 *   distributed crawl, a listing given up on after this long ends the unit, which hands back what it has not listed.
 *   On S3 this covers every call (listing, fetching, moving), a listing resumes from the page that failed, and the
 *   SDK's own retries are off unless this is 0. A listing that gets a page through starts its time again, so a long
 *   listing that fails now and then is not given up on.</li>
 *   <li>sourceRetryCapSecs (Int, Optional) : The longest single wait between those retries. Defaults to 20.</li>
 *   <li>partitioning.handBackGroupSize (Int, Optional) : How many of the directories a unit hands back make one new
 *   unit. Defaults to maxDirectoriesPerUnit, or 64 when that is not set. A storage client that walks a unit's
 *   directories in parallel can take groups of hundreds or thousands.</li>
 *   <li>gcp.pathToServiceKey (String, Required) : Path to the Google Cloud service key JSON.</li>
 *   <li>gcp.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>s3.accessKeyId (String, Optional) : AWS access key ID (omit to use default credentials).</li>
 *   <li>s3.secretAccessKey (String, Optional) : AWS secret access key (omit to use default credentials).</li>
 *   <li>s3.region (String, Optional) : AWS region for S3.</li>
 *   <li>s3.anonymous (Boolean, Optional) : Send unsigned requests, for public buckets that need no
 *   credentials. Cannot be combined with accessKeyId/secretAccessKey. When set and no region is given,
 *   region defaults to us-east-1, S3's global endpoint. Defaults to false.</li>
 *   <li>s3.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>azure.connectionString (String, Optional) : Azure connection string.</li>
 *   <li>azure.accountName (String, Optional) : Azure account name.</li>
 *   <li>azure.accountKey (String, Optional) : Azure account key.</li>
 *   <li>azure.maxNumOfPages (Int, Optional) : Maximum number of file references to hold in memory. Defaults to 100.</li>
 *   <li>fileHandlers (Map&lt;String, Map&lt;String, Object&gt;&gt;, Optional) : Per-type FileHandler configuration (e.g., csv, json, xml).
 *   Supply a class to override the default handler. Otherwise the built-in handler for csv/json/xml is used. Configure docIdPrefix inside
 *   each handler's config as needed.</li>
 * </ul>
 */
public class FileConnector extends AbstractConnector implements PartitionableConnector {

  // keys of a work unit's payload
  private static final String UNIT_PATH = "path";
  // for a unit that covers several directories, in place of UNIT_PATH
  private static final String UNIT_PATHS = "paths";
  private static final String UNIT_RECURSIVE = "recursive";
  // how many directories are handed back as one unit when no limit on directories says how many a unit may list
  private static final int DEFAULT_HAND_BACK_GROUP_SIZE = 64;

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";
  public static final String EXPIRED = "file_expired";
  public static final String ARCHIVE_FILE_SEPARATOR = "!";

  // cloudOption Keys
  public static final String AZURE_CONNECTION_STRING = "connectionString";
  public static final String AZURE_ACCOUNT_NAME = "accountName";
  public static final String AZURE_ACCOUNT_KEY = "accountKey";
  public static final String S3_REGION = "region";
  public static final String S3_ACCESS_KEY_ID = "accessKeyId";
  public static final String S3_SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String S3_ANONYMOUS = "anonymous";
  public static final String GOOGLE_SERVICE_KEY = "pathToServiceKey";
  public static final String MAX_NUM_OF_PAGES = "maxNumOfPages";

  // fileOption Config Options
  public static final String GET_FILE_CONTENT = "getFileContent";
  public static final String HANDLE_ARCHIVED_FILES = "handleArchivedFiles";
  public static final String HANDLE_COMPRESSED_FILES = "handleCompressedFiles";
  public static final String MOVE_TO_AFTER_PROCESSING = "moveToAfterProcessing";
  public static final String MOVE_TO_ERROR_FOLDER = "moveToErrorFolder";

  // parent specs for cloud provider configs
  public static final Spec GCP_PARENT_SPEC = SpecBuilder.parent("gcp")
      .requiredString("pathToServiceKey")
      .optionalNumber("maxNumOfPages").build();
  public static final Spec S3_PARENT_SPEC = SpecBuilder.parent("s3")
      .optionalString("accessKeyId", "secretAccessKey", "region")
      .optionalBoolean("anonymous")
      .optionalNumber("maxNumOfPages").build();
  public static final Spec AZURE_PARENT_SPEC = SpecBuilder.parent("azure")
      .optionalString("connectionString", "accountName", "accountKey")
      .optionalNumber("maxNumOfPages").build();

  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredList("paths", new TypeReference<List<String>>(){})
      .optionalBoolean("concurrent")
      .optionalNumber(SourceRetryPolicy.RETRY_SECS, SourceRetryPolicy.RETRY_CAP_SECS)
      .optionalParent(
          SpecBuilder.parent("filterOptions")
              .optionalList("includes", new TypeReference<List<String>>(){})
              .optionalList("excludes", new TypeReference<List<String>>(){})
              .optionalList("pathsToSkip", new TypeReference<List<String>>(){})
              // durations are strings.
              .optionalString("lastModifiedCutoff", "lastPublishedCutoff", "publishMode", "sendTombstones").build(),
          SpecBuilder.parent("fileOptions")
              .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
              .optionalString("moveToAfterProcessing", "moveToErrorFolder").build(),
          SpecBuilder.parent("state")
              .optionalString("driver", "connectionString", "jdbcUser", "jdbcPassword", "tableName")
              .optionalBoolean("performDeletions", "enabled")
              .optionalNumber("pathLength", "runsBeforeExpiration").build(),
          SpecBuilder.parent("partitioning")
              .optionalNumber("depth", "maxDirectoriesPerUnit", "maxUnitSecs", "handBackGroupSize").build(),
          GCP_PARENT_SPEC,
          AZURE_PARENT_SPEC,
          S3_PARENT_SPEC)
      .optionalParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){}).build();

  private final List<URI> storageURIs;
  private final Map<String, StorageClient> storageClientMap;

  private final FileConnectorStateManager stateManager;

  private final boolean concurrent;

  // Set once this instance has executed a work unit, and so has seen only part of the traversal.
  private boolean executedUnit = false;

  public FileConnector(Config config) throws ConnectorException {
    super(config);

    this.concurrent = ConfigUtils.getOrDefault(config, "concurrent", false);
    // checked here rather than at the first traversal, so that a bad value is found before the run starts
    SourceRetryPolicy.fromConfig(config);

    List<String> paths = config.getStringList("paths");
    this.storageURIs = new ArrayList<>();

    for (String path : paths) {
      try {
        URI newStorageURI = TraversalParams.parsePathOrURI(path);
        storageURIs.add(newStorageURI);
        log.debug("FileConnector to use path {} with scheme {}", path, newStorageURI.getScheme());
      } catch (URISyntaxException e) {
        throw new ConnectorException("Invalid path to storage: " + path, e);
      }
    }

    this.stateManager =
        config.hasPath("state") && ConfigUtils.getOrDefault(config, "state.enabled", true)
            ? new FileConnectorStateManager(config.getConfig("state"), getName())
            : null;

    // FileConnector retries refused calls by default, where other users of storage clients keep the SDK's retries
    this.storageClientMap = StorageClient.createClients(config.withFallback(ConfigFactory.parseMap(
        Map.of(SourceRetryPolicy.RETRY_SECS, SourceRetryPolicy.DEFAULT_RETRY_SECS))));


    // incremental mode requires state tracking in order to function correctly
    if (config.hasPath("filterOptions.publishMode")) {
      PublishMode mode = PublishMode.fromString(config.getString("filterOptions.publishMode"));
      if (mode == PublishMode.INCREMENTAL && !config.hasPath("state")) {
        throw new IllegalArgumentException("filterOptions.publishMode of 'incremental' requires state configuration.");
      }
    }

    if (config.hasPath("filterOptions.sendTombstones") && config.getBoolean("filterOptions.sendTombstones") &&
        (!config.hasPath("filterOptions.publishMode") ||
            PublishMode.fromString(config.getString("filterOptions.publishMode")) == PublishMode.FULL)) {
      throw new IllegalArgumentException("publishMode must be set and be incremental to use the sendTombstones toggle.");
    }

    // Cannot specify multiple storage paths and a moveTo of some kind
    if (storageURIs.size() > 1 && (config.hasPath("fileOptions.moveToAfterProcessing") || config.hasPath("fileOptions.moveToErrorFolder"))) {
      throw new IllegalArgumentException("FileConnector does not support multiple paths and moveToAfterProcessing / moveToErrorFolder. Create individual FileConnectors.");
    }

    // A collapsing Publisher is not safe for concurrent publish() calls - it would merge and drop documents silently
    if (concurrent && requiresCollapsingPublisher()) {
      throw new IllegalArgumentException("FileConnector does not support concurrent traversal and collapse. Disable one of them.");
    }

    if (concurrent) {
      validateNonOverlappingPaths();
    }

    if (config.hasPath("filterOptions.lastPublishedCutoff") && !config.hasPath("state")) {
      log.warn("filterOptions.lastPublishedCutoff was specified, but no state configuration was provided. It will not be enforced.");
    }

    if (isPartitioningEnabled()) {
      if (partitioningDepth() < 0) {
        throw new IllegalArgumentException("partitioning.depth cannot be negative.");
      }
      for (String limit : List.of("partitioning.maxDirectoriesPerUnit", "partitioning.maxUnitSecs",
          "partitioning.handBackGroupSize")) {
        if (config.hasPath(limit) && config.getInt(limit) < 1) {
          throw new IllegalArgumentException(limit + " must be at least 1.");
        }
      }
    }
  }

  @Override
  public boolean isPartitioningEnabled() {
    return config.hasPath("partitioning");
  }

  // getInt accepts a number written as a String, which is what a value substituted from the environment is
  private int partitioningDepth() {
    return config.hasPath("partitioning.depth") ? config.getInt("partitioning.depth") : 1;
  }

  /**
   * Marks every file in the state database as not yet encountered, ahead of the units marking the files they find.
   */
  @Override
  public void prepareRun(String runId) throws ConnectorException {
    // Units run on different Crawlers, which could not share the embedded database that state defaults to.
    if (stateManager != null && !config.hasPath("state.connectionString")) {
      throw new ConnectorException("A partitioned FileConnector that uses state requires state.connectionString.");
    }

    initialize();
  }

  /**
   * Splits each path into units: one for each directory <code>partitioning.depth</code> levels down, to be traversed
   * recursively, and one for the files directly in each directory above that level.
   */
  @Override
  public void plan(String runId, WorkUnitSink sink) throws ConnectorException {
    initializeStorageClients();
    int depth = partitioningDepth();

    for (URI resource : storageURIs) {
      planPath(resource, buildTraversalParams(resource), depth, sink);
    }
  }

  private void planPath(URI path, TraversalParams params, int depth, WorkUnitSink sink) throws ConnectorException {
    List<URI> subdirectories = null;

    if (depth > 0) {
      try {
        subdirectories = getStorageClient(path).listSubdirectories(path, params);
      } catch (UnsupportedOperationException e) {
        log.info("Path {} will be traversed as a single unit: {}", path, e.getMessage());
      } catch (IOException e) {
        throw new ConnectorException("Error listing the directories under " + path, e);
      }
    }

    if (subdirectories == null) {
      sink.emit(path.toString(), WorkUnit.newPayload().put(UNIT_PATH, path.toString()).put(UNIT_RECURSIVE, true));
      return;
    }

    sink.emit(path + "#files", WorkUnit.newPayload().put(UNIT_PATH, path.toString()).put(UNIT_RECURSIVE, false));
    for (URI subdirectory : subdirectories) {
      planPath(subdirectory, params, depth - 1, sink);
    }
  }

  @Override
  public void executeUnit(WorkUnit unit, Publisher publisher, UnitContext context) throws ConnectorException {
    List<URI> unitPaths = parseUnitPaths(unit);
    boolean recursive = unit.payload().path(UNIT_RECURSIVE).asBoolean(true);
    executedUnit = true;

    initializeStorageClients();
    // one budget for the whole unit: once it is used up, the directories not yet reached are handed back unlisted,
    // a group at a time as the groups fill, and what is left of a group at each progress report from then on
    AtomicReference<TraversalBudget> budgetRef = new AtomicReference<>();
    HandBacks handBacks = new HandBacks(unitPaths, recursive, handBackGroupSize(), context,
        () -> budgetRef.get() != null && budgetRef.get().isUsedUp());
    TraversalBudget budget = newUnitBudget(context, handBacks);
    budgetRef.set(budget);
    context.onProgress(handBacks::flushPartial);

    try {
      if (stateManager != null) {
        stateManager.openForPartialTraversal();
      }
      traverseWithinBudget(publisher, unitPaths, recursive, budget);
    } catch (ClassNotFoundException | SQLException e) {
      throw new ConnectorException("Error connecting to the state database.", e);
    } finally {
      if (stateManager != null) {
        stateManager.closeStateForThread();
      }
    }

    // listings, refusals, a failure of the source and the full groups went to the context as they happened
    handBacks.finish();
    long limitReached = budget.limitReachedMillis();
    if (limitReached > 0) {
      context.recordTimePastBound(Math.max(0, System.currentTimeMillis() - limitReached));
    }
  }

  // The paths of a unit are given to each storage client together, so that a client able to walk several at once
  // can. Paths in different providers go to different clients, one after another.
  private void traverseWithinBudget(Publisher publisher, List<URI> paths, boolean recursive, TraversalBudget budget)
      throws ConnectorException {
    Map<StorageClient, List<TraversalParams>> byClient = new LinkedHashMap<>();
    for (URI path : paths) {
      byClient.computeIfAbsent(getStorageClient(path), client -> new ArrayList<>())
          .add(new TraversalParams(config, path, getDocIdPrefix(), recursive, budget));
    }

    for (Map.Entry<StorageClient, List<TraversalParams>> entry : byClient.entrySet()) {
      try {
        entry.getKey().traverseAll(publisher, entry.getValue(), stateManager);
      } catch (ConnectorException e) {
        throw e;
      } catch (Exception e) {
        throw new ConnectorException("Error occurred while traversing " + entry.getValue().get(0).getURI()
            + (entry.getValue().size() > 1 ? " and " + (entry.getValue().size() - 1) + " more" : "") + ".", e);
      }
    }
  }

  /**
   * Hands back the directories a unit found and did not walk. They are handed back in groups, each to be one unit,
   * of as many directories as a unit may list. One unit for each directory would be simpler, but what a unit leaves
   * over is mostly small: the siblings of the directories it was in when it stopped. Units of one small directory
   * each cost more to dispatch than to execute.
   *
   * Each group goes to the context as soon as it is full, so that the Crawler can have it dispatched while the unit
   * is still executing. A partial group goes when the unit ends, or, once the unit has reached its limit, at the next
   * progress report: a unit past its limit only finishes listings already under way, which can take minutes, and a
   * part that hands back fewer directories than a group would otherwise hold all of them back that long. Until the
   * limit is reached, and without a limit, the groups are the ones handing every directory back at the end would
   * make, so the same directories handed back in the same order get the same keys.
   */
  static final class HandBacks {

    private final List<URI> unitPaths;
    private final boolean recursive;
    private final int groupSize;
    private final UnitContext context;
    private final BooleanSupplier usedUp;
    private final List<URI> directories = new ArrayList<>();
    private int handedBackUpTo = 0;

    /**
     * @param usedUp whether the unit has reached its limit, after which a partial group need not wait to fill.
     */
    HandBacks(List<URI> unitPaths, boolean recursive, int groupSize, UnitContext context, BooleanSupplier usedUp) {
      this.unitPaths = unitPaths;
      this.recursive = recursive;
      this.groupSize = groupSize;
      this.context = context;
      this.usedUp = usedUp;
    }

    /** Hands back the partial group waiting to fill, if the unit has reached its limit. Run at each progress report. */
    synchronized void flushPartial() {
      if (handedBackUpTo < directories.size() && usedUp.getAsBoolean()) {
        handBackGroup(directories.subList(handedBackUpTo, directories.size()));
        handedBackUpTo = directories.size();
      }
    }

    synchronized void add(URI directory) {
      directories.add(directory);
      if (directories.size() - handedBackUpTo >= groupSize) {
        handBackGroup(directories.subList(handedBackUpTo, handedBackUpTo + groupSize));
        handedBackUpTo += groupSize;
      }
    }

    synchronized void finish() {
      for (int from = handedBackUpTo; from < directories.size(); from += groupSize) {
        handBackGroup(directories.subList(from, Math.min(from + groupSize, directories.size())));
      }
      handedBackUpTo = directories.size();
    }

    private void handBackGroup(List<URI> group) {
      String first = group.get(0).toString();

      // A directory found beneath the unit's own is walked whole when it is handed back. One of the unit's own
      // directories, handed back because it could not be listed, is handed back as the unit had it: a unit for the
      // files in a directory must not come back as a unit for its whole subtree.
      boolean recursiveGroup = recursive || !unitPaths.containsAll(group);
      String keySuffix = recursiveGroup ? "" : "#files";

      // a single directory is described as the planner would describe it, and so is the same unit as the planner's
      if (group.size() == 1) {
        context.handBack(first + keySuffix, WorkUnit.newPayload().put(UNIT_PATH, first).put(UNIT_RECURSIVE, recursiveGroup));
        return;
      }

      ObjectNode payload = WorkUnit.newPayload().put(UNIT_RECURSIVE, recursiveGroup);
      ArrayNode paths = payload.putArray(UNIT_PATHS);
      group.forEach(directory -> paths.add(directory.toString()));
      // the key has to be the same whenever the same directories are handed back together, and different otherwise
      String key = first + "+" + (group.size() - 1) + keySuffix + "~" + DigestUtils.sha256Hex(payload.toString()).substring(0, 16);
      context.handBack(key, payload);
    }
  }

  // How many directories are handed back as one unit: as many as a unit may list, unless the config says otherwise.
  // A client that walks a unit's directories in parallel does well with far larger groups than it lists in sequence.
  private int handBackGroupSize() {
    for (String setting : List.of("partitioning.handBackGroupSize", "partitioning.maxDirectoriesPerUnit")) {
      if (config.hasPath(setting)) {
        return config.getInt(setting);
      }
    }
    return DEFAULT_HAND_BACK_GROUP_SIZE;
  }

  // The limits on one unit's traversal. With neither limit configured the budget only counts directories listed.
  private TraversalBudget newUnitBudget(UnitContext context, HandBacks handBacks) {
    Integer maxDirectories = config.hasPath("partitioning.maxDirectoriesPerUnit")
        ? config.getInt("partitioning.maxDirectoriesPerUnit") : null;
    Long maxMillis = config.hasPath("partitioning.maxUnitSecs")
        ? TimeUnit.SECONDS.toMillis(config.getInt("partitioning.maxUnitSecs")) : null;
    // a unit that has been given up on stops listing as well
    TraversalBudget budget = new TraversalBudget(maxDirectories, maxMillis, context::isCancelled);
    // the unit's account is kept as it goes, so that the Crawler's progress reports carry it
    budget.setListener(new TraversalBudget.Listener() {
      @Override
      public void listed() {
        context.addSourceCalls(1);
      }

      @Override
      public void refused(FailureClass failureClass) {
        context.addRefusedCalls(1, failureClass);
      }

      @Override
      public void requested() {
        context.addRequests(1);
      }

      @Override
      public void directoryPaged(long pages) {
        context.recordDirectoryPages(pages);
      }

      // before the directory the source failed on is handed back, so that the Crawler holds that back
      @Override
      public void sourceError(TraversalBudget.SourceError error) {
        context.recordSourceError(error.failureClass(), error.cause());
      }

      @Override
      public void handedBack(URI directory) {
        handBacks.add(directory);
      }
    });
    budget.setMaxConcurrency(context::maxSourceConcurrency);
    return budget;
  }

  /**
   * Returns the paths a unit asks to have traversed: the one it names, or each of several.
   */
  private List<URI> parseUnitPaths(WorkUnit unit) throws ConnectorException {
    JsonNode paths = unit.payload().path(UNIT_PATHS);
    if (paths.isMissingNode()) {
      return List.of(parseUnitPath(unit, unit.payload().path(UNIT_PATH).asText()));
    }
    if (!paths.isArray() || paths.isEmpty()) {
      throw new ConnectorException("Work unit " + unit.unitId() + " names no paths.");
    }

    List<URI> unitPaths = new ArrayList<>();
    for (JsonNode path : paths) {
      unitPaths.add(parseUnitPath(unit, path.asText()));
    }
    return unitPaths;
  }

  /**
   * Returns a path that a unit asks to have traversed. Units arrive over the network, so a path is only accepted if
   * it lies within one of this connector's configured paths.
   */
  private URI parseUnitPath(WorkUnit unit, String path) throws ConnectorException {
    URI unitPath;
    try {
      unitPath = new URI(path).normalize();
    } catch (URISyntaxException e) {
      throw new ConnectorException("Work unit " + unit.unitId() + " has an invalid path.", e);
    }

    for (URI configured : storageURIs) {
      StorageClient client = storageClientMap.get(clientKeyFor(configured));

      if (client != null && clientKeyFor(configured).equals(clientKeyFor(unitPath)) && isWithin(client, configured, unitPath)
          && !isSkipped(client, configured, unitPath)) {
        return unitPath;
      }
    }

    throw new ConnectorException("Work unit " + unit.unitId() + " has a path outside this connector's configured paths.");
  }

  // Whether a traversal of the configured path could have been split into a unit for the given path.
  private static boolean isWithin(StorageClient client, URI configured, URI unitPath) {
    switch (clientKeyFor(configured)) {
      case "file":
        return client.containsPath(configured, unitPath) && isReallyWithin(configured, unitPath);
      case "s3":
        // Keys are extended as prefixes, without regard to '/'. A traversal of s3://bucket/data also covers
        // s3://bucket/data-old/, so the planner emits a unit for it and it has to be accepted here.
        return Objects.equals(configured.getAuthority(), unitPath.getAuthority())
            && unitPath.getPath().startsWith(configured.normalize().getPath());
      default:
        // paths in other providers are not split, so the only unit there can be is for the configured path itself
        return unitPath.equals(configured.normalize());
    }
  }

  // A path can sit inside the configured one as written and still lead out of it, if one of its directories is a
  // symbolic link. A traversal does not follow links, so no unit is planned for such a path. Compares where the two
  // paths actually are; a path that does not exist is not within anything.
  private static boolean isReallyWithin(URI configured, URI unitPath) {
    try {
      return toLocalPath(unitPath).toRealPath().startsWith(toLocalPath(configured).toRealPath());
    } catch (IOException | RuntimeException e) {
      return false;
    }
  }

  private static Path toLocalPath(URI uri) {
    return uri.isAbsolute() ? Paths.get(uri) : Paths.get(uri.getPath());
  }

  // Whether the path is, or lies under, a directory that filterOptions.pathsToSkip excludes from the traversal.
  // The planner never emits such a path, since a traversal turns back at a skipped directory.
  private boolean isSkipped(StorageClient client, URI configured, URI unitPath) {
    for (URI skipped : buildTraversalParams(configured).getPathsToSkip()) {
      boolean under = "file".equals(clientKeyFor(unitPath))
          ? "file".equals(clientKeyFor(skipped)) && client.containsPath(skipped, unitPath)
          : withTrailingSlash(unitPath.toString()).startsWith(withTrailingSlash(skipped.toString()));

      if (under) {
        return true;
      }
    }

    return false;
  }

  private static String withTrailingSlash(String s) {
    return s.endsWith("/") ? s : s + "/";
  }

  /**
   * Applies the parts of a traversal that need to know every file that was encountered: files that no unit saw
   * move a step closer to expiring, and tombstones are published for those that have expired.
   */
  @Override
  public void finalizeRun(String runId, Publisher publisher) throws ConnectorException {
    if (stateManager == null) {
      return;
    }

    try {
      // a Coordinator that resumed the run has not connected yet
      stateManager.connect();
      stateManager.incrementRunsNotEncountered();
    } catch (ClassNotFoundException | SQLException e) {
      throw new ConnectorException("Error finalizing state after traversal.", e);
    }

    if (config.hasPath("filterOptions.sendTombstones") && config.getBoolean("filterOptions.sendTombstones")) {
      sendExpiredFileTombstones(publisher);
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    initialize();

    // discover and publish all valid file candidates
    try {
      if (concurrent && storageURIs.size() > 1) {
        traversePathsConcurrently(publisher);
      } else {
        for (URI resource : storageURIs) {
          traverseStoragePath(publisher, resource);
        }
      }
    } finally {
      if (stateManager != null) {
        stateManager.closeStateForThread();
      }
    }

    // bump runs_not_encountered so listExpiredFiles and shutdown's deletion see up-to-date counters
    if (stateManager != null) {
      try {
        stateManager.incrementRunsNotEncountered();
      } catch (SQLException e) {
        throw new ConnectorException("Error finalizing state after traversal.", e);
      }
    }

    if (config.hasPath("filterOptions.sendTombstones") &&
        config.getBoolean("filterOptions.sendTombstones")) {
      // find files no longer in datastore that need to be removed from index
      sendExpiredFileTombstones(publisher);
    }
  }

  // stateful only: publish tombstones for files seen during prior ingests but not the current
  private void sendExpiredFileTombstones(Publisher publisher) throws ConnectorException {
    // skip if state not being managed
    if (stateManager == null) {
      return;
    }

    List<URI> expiredFileUris = null;
    try {
      expiredFileUris = stateManager.listExpiredFiles();
    } catch (SQLException e) {
      log.warn("Error occurred while publishing missing document tombstones.", e);
      return;
    }

    if (expiredFileUris.isEmpty()) {
      return;
    }
    int expiredFileCount = expiredFileUris.size();
    int publishedTombstoneCount = 0;
    log.info("{} previously published files now missing/expired, publishing document tombstones...", expiredFileCount);
    for (URI uri : expiredFileUris) {
      Document doc = buildTombstoneDoc(uri);
      try {
        publisher.publish(doc);
        publishedTombstoneCount++;
      } catch (Exception e) {
        throw new ConnectorException("Error publishing document tombstone for file: " + uri, e);
      }
    }
    log.info("Published {} of {} document tombstones for tracking and index removal", publishedTombstoneCount, expiredFileCount);

  }

  private void initializeStorageClients() throws ConnectorException {
    try {
      for (StorageClient client : storageClientMap.values()) {
        client.init();
      }
    } catch (IOException e) {
      throw new ConnectorException("Error initializing a StorageClient.", e);
    }
  }

  private void initialize() throws ConnectorException {
    initializeStorageClients();
    if (stateManager != null) {
      try {
        stateManager.init();
      } catch (Exception e) {
        throw new ConnectorException("Error occurred initializing StorageClientStateManager.", e);
      }
    }
  }

  @Override
  public void close() {
    if (stateManager != null && executedUnit) {
      // other units of the traversal ran elsewhere, so this instance cannot tell which files have expired
      stateManager.closeForPartialTraversal();
    } else if (stateManager != null) {
      try {
        stateManager.shutdown();
      } catch (SQLException e) {
        log.warn("Error occurred while shutting down FileConnectorStateManager.", e);
      }
    }
    for (StorageClient client : storageClientMap.values()) {
      try {
        client.shutdown();
      } catch (IOException e) {
        log.warn("Error shutting down StorageClient.", e);
      }
    }
  }

  /**
   * Traverses each of the storage paths on its own thread. Requires the paths to not overlap, so that concurrent
   * traversals never touch the same row of the state database.
   */
  private void traversePathsConcurrently(Publisher publisher) throws ConnectorException {
    ThreadFactory threadFactory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("PathTraversal") + "-%d")
        .build();
    ExecutorService executor = Executors.newFixedThreadPool(storageURIs.size(), threadFactory);

    try {
      List<Future<?>> traversals = new ArrayList<>();

      for (URI resource : storageURIs) {
        traversals.add(executor.submit(() -> {
          traverseStoragePathOnThisThread(publisher, resource);
          return null;
        }));
      }

      // Every traversal runs to completion, even after one fails. Aborting the others would leave some of this run's
      // files unmarked in the state database, and they would then be expired, tombstoned and deleted.
      ConnectorException failure = null;

      // traversals is in the same order as storageURIs, so the index identifies the path that failed
      for (int i = 0; i < traversals.size(); i++) {
        URI resource = storageURIs.get(i);

        try {
          traversals.get(i).get();
          log.info("Finished traversing '{}'.", resource);
        } catch (ExecutionException e) {
          log.error("Error occurred while traversing '{}', CONTINUING", resource, e.getCause());

          if (failure == null) {
            failure = new ConnectorException("One or more traversals failed.", e.getCause());
          } else {
            failure.addSuppressed(e.getCause());
          }
        }
      }

      if (failure != null) {
        throw failure;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectorException("Interrupted while waiting for traversals to finish.", e);
    } finally {
      executor.shutdownNow();
    }
  }

  // Gives the calling thread its own TraversalState for the traversal. A JDBC Connection is not thread-safe, so each
  // traversal thread needs its own.
  private void traverseStoragePathOnThisThread(Publisher publisher, URI pathToTraverse) throws Exception {
    if (stateManager == null) {
      traverseStoragePath(publisher, pathToTraverse);
      return;
    }

    stateManager.openStateForThread();

    try {
      traverseStoragePath(publisher, pathToTraverse);
    } finally {
      stateManager.closeStateForThread();
    }
  }

  /**
   * Rejects overlapping paths. Threads traversing overlapping paths would touch the same rows of the state database,
   * and would publish the files under the shared paths more than once.
   * <p> A StorageClient can only compare paths within its own storage provider, and cannot detect paths
   * that reach the same files by another route, such as a symlink or an S3 access point alias.
   */
  private void validateNonOverlappingPaths() {
    for (int i = 0; i < storageURIs.size(); i++) {
      for (int j = i + 1; j < storageURIs.size(); j++) {
        URI first = storageURIs.get(i);
        URI second = storageURIs.get(j);
        StorageClient client = storageClientMap.get(clientKeyFor(first));

        // paths handled by different storage providers cannot overlap
        if (client == null || client != storageClientMap.get(clientKeyFor(second))) {
          continue;
        }

        if (client.containsPath(first, second) || client.containsPath(second, first)) {
          throw new IllegalArgumentException("FileConnector cannot traverse overlapping paths concurrently: '"
              + first + "' and '" + second + "'.");
        }
      }
    }
  }

  private static String clientKeyFor(URI pathToTraverse) {
    return pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
  }

  private void traverseStoragePath(Publisher publisher, URI pathToTraverse) throws ConnectorException {
    traverseStoragePath(publisher, pathToTraverse, true);
  }

  private StorageClient getStorageClient(URI path) throws ConnectorException {
    StorageClient storageClient = storageClientMap.get(clientKeyFor(path));

    if (storageClient == null) {
      throw new ConnectorException("No StorageClient was available for (" + path +
          "). Did you include the necessary configuration?");
    }

    return storageClient;
  }

  private void traverseStoragePath(Publisher publisher, URI pathToTraverse, boolean recursive) throws ConnectorException {
    StorageClient storageClient = getStorageClient(pathToTraverse);
    TraversalParams params = new TraversalParams(config, pathToTraverse, getDocIdPrefix(), recursive);

    try {
      storageClient.traverse(publisher, params, stateManager);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while traversing " + pathToTraverse + ".", e);
    }
  }

  private TraversalParams buildTraversalParams(URI pathToTraverse) {
    return new TraversalParams(config, pathToTraverse, getDocIdPrefix());
  }

  /**
   * Builds a tombstone Document for the given URI. The document is marked as expired and skipped,
   * so it bypasses pipeline stages and signals downstream indexers to delete the corresponding entry.
   *
   * @param uri the URI of the file that is no longer present in storage
   * @return a Document with {@link #EXPIRED} set to {@code true} and marked as skipped
   */
  private Document buildTombstoneDoc(URI uri) {
    Instant now = Instant.now();
    Document doc = BaseFileReference.buildBaseDoc(uri.toString(), now, 0L, now, buildTraversalParams(uri));
    doc.setField(EXPIRED, true);
    doc.setSkipped(true);
    return doc;
  }

}
