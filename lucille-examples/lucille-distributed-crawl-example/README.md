# Distributed Crawl Example

A small Connector, written from scratch, that takes part in a [distributed crawl](../../doc/site/content/en/docs/architecture/overview/distributed-crawl.md): it publishes one Document per file under a local directory, and splits that work into **work units** that many Crawler processes execute.

Lucille's own `FileConnector` already does this, for local paths and S3, with more options. This example exists to show the mechanics in about two hundred lines, so that you can write a Connector of your own for a source that `FileConnector` does not cover.

## What is here

| Path | What it is |
|---|---|
| `src/main/java/.../DirectoryTreeConnector.java` | The Connector. Read this first. |
| `src/main/java/.../InProcessDemo.java` | Runs a whole distributed crawl in one JVM, with in-memory queues in place of Kafka. |
| `src/main/java/.../SampleTree.java` | Writes a small, lopsided tree to crawl: 211 files, most of them under `big/`. |
| `src/test/java/.../DirectoryTreeConnectorTest.java` | How to test such a Connector: by calling it directly and playing the Coordinator's part. |
| `conf/crawl.conf` | The crawl's config, shared by every role. |
| `conf/kafka.conf` | The same, with Kafka settings, for separate processes. |
| `scripts/run-in-process.sh` | Runs `InProcessDemo`. |
| `scripts/run-kafka.sh` | Runs two Crawlers, a Worker, an Indexer and a Coordinator as separate processes. |

## Running it

```bash
mvn package                        # from this directory; or `mvn install` from the repository root first
./scripts/run-in-process.sh        # no broker needed
```

The output is `target/crawl-output.csv`, one row per file. The log shows each unit the Coordinator dispatches, which Crawler executed it, and how many directories it handed back.

Across processes, with a Kafka broker at `localhost:9092` (or `KAFKA_BOOTSTRAP_SERVERS`):

```bash
docker run -d --name crawl-example-kafka -p 9092:9092 apache/kafka:4.2.0
./scripts/run-kafka.sh
grep 'done by' target/logs/coordinator.log | sed -E 's/.*done by (Crawler-[a-z0-9]+).*/\1/' | sort | uniq -c
```

The last line shows how the units were spread over the two Crawler processes. On the sample tree a run makes about a hundred units, split close to evenly. If your broker does not create topics automatically, create `crawl_pipeline_source` and `crawl_pipeline_dest` first.

## The mechanics, in the order they matter

A Connector takes part in a distributed crawl by implementing `PartitionableConnector`. The Coordinator calls `plan()` once per run; Crawlers call `executeUnit()` for each unit. Everything else (dispatching units, tracking Documents, retrying failures, resuming after a crash) is Lucille's.

### 1. Plan units with stable keys

```java
public void plan(String runId, WorkUnitSink sink) {
  sink.emit("#files", unitPayload(root, false));              // the files directly in the root
  for (Path directory : subdirectories(root)) {
    sink.emit(keyFor(directory), unitPayload(directory, true)); // one unit per directory beneath it
  }
}
```

A unit is a **key** and a small JSON **payload** saying *where* to read. The key must name the same part of the source every time: a Coordinator that resumes an interrupted run calls `plan()` again and skips the keys it already has. Here the key is the path relative to the root.

### 2. Execute a unit, publishing stable Document IDs

A unit can be executed more than once: if it fails, if its Crawler dies, if Kafka moves its partition. Each execution must publish the same Document IDs, so that the search engine ends up with one copy. Here the ID is the file's path relative to the root, whichever unit found it.

### 3. Check what the unit asks for

Units are records on a Kafka topic. A Crawler must not read wherever a record tells it to, so `checkedPath()` accepts a path only if its real path lies inside the configured root, and the walk skips symbolic links.

### 4. Bound the unit, and hand back the rest

Without a bound, the unit for `big/` would walk all of it on one Crawler while the others sat idle. Instead a unit lists at most `partitioning.maxDirectoriesPerUnit` directories, breadth first, publishing their files, and hands the directories it **found but did not list** back:

```java
for (Path directory : notListed) {
  context.handBack(keyFor(directory), unitPayload(directory, true));
}
```

The Crawler sends them to the Coordinator with its next progress report, every heartbeat, while the unit is still running; the Coordinator makes a unit of each and dispatches it to whichever Crawler is free. Those units are bounded the same way, so a subtree of any size is shared out. Two rules make this safe:

- **Hand back, don't do, what you hand back.** A handed-back part is executed by whoever receives it.
- **Use the keys the planner would use.** Then a directory that is both planned and handed back is one unit, not two; the Coordinator ignores a key it already has.

`FileConnector` goes further: it hands directories back in groups, so that a unit is never a single tiny directory, and it applies a time limit as well as a count.

### 5. Report, and stop when told

`context.addSourceCalls(1)` per listing gives the Coordinator a measure of each unit's cost, which `-costsFrom` uses on the next run to start the largest units first. `context.isCancelled()` turns true when the unit has timed out, lost its partition or lost its run; the walk then stops and hands nothing back, since the unit will be executed again from the start.

A Connector reading a source that can refuse requests would also count refusals with `context.addRefusedCalls(n, FailureClass.THROTTLED)` for a 429, or `SOURCE_UNAVAILABLE` for a 5xx, so the Coordinator can slow the run down, and could cap its own concurrency at `context.maxSourceConcurrency()`. This one reads a local disk and has no need to.

## Testing a Connector like this

`DirectoryTreeConnectorTest` shows the approach: call `plan()`, execute each unit with a `UnitContext` that records what it hands back, turn what it hands back into units (skipping keys already seen, as the Coordinator does), and repeat until nothing is left. Then check that every file was published **exactly once**, for several bounds, from one directory per unit to unbounded. That property is what a distributed crawl depends on, and it is easy to break when changing how a unit walks or hands back.
