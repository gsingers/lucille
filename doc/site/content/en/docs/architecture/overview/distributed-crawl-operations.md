---
title: "Running a Distributed Crawl"
weight: 6
date: 2026-10-06
description: >
  How to start, configure, size, secure and operate a distributed crawl.
---

This page is about operating a distributed crawl. [Distributing the Crawl]({{< relref "docs/architecture/overview/distributed-crawl" >}}) explains how it works.

## Starting the components

All components share one config file. Start the Crawlers, Workers and Indexer first; they wait for work.

```bash
# any number of these, on any number of machines
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Crawler
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Worker <pipeline-name>
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Indexer <pipeline-name>

# one per run
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -distributedCrawl
```

With the Docker image, set `LUCILLE_ROLE=crawler` for a Crawler, and `LUCILLE_OPTS=-distributedCrawl` for the Runner.

| Runner flag | Meaning |
|---|---|
| `-distributedCrawl` | Run as the Coordinator of a distributed crawl. |
| `-runId <id>` | Use this run ID instead of generating one. Works in every mode. For a distributed crawl it may contain only letters, digits, `.`, `_` and `-`, up to 128 characters. |
| `-resume <id>` | Continue the unfinished distributed crawl with this run ID. Refused if the run's heartbeat is recent. |
| `-force` | With `-resume`, take over even if the run's heartbeat is recent. |
| `-resumeIfExists` | With `-distributedCrawl` and `-runId`: start the run if it is new, continue it if not. For a Runner that is restarted automatically. |
| `-listRuns` | List the distributed crawls on record, with the state of each, and exit. |
| `-costsFrom <id>` | With `-distributedCrawl` or `-resume`: dispatch the largest units first, as measured by this earlier run of the same config. |

## A minimal config

Add a `partitioning` block to the Connector. The `crawl` block is optional; `application-example.conf` lists every setting and its default.

```hocon
connectors: [
  {
    name: "docs"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "pipeline1"
    paths: ["s3://my-bucket/docs/"]
    s3 { region: "us-east-1" }
    partitioning { depth: 0, maxDirectoriesPerUnit: 500 }
  }
]

crawl {
  workTopicPartitions: 16   # one unit in flight per partition; match the total Crawler threads
  threads: 4                # units executed at once by each Crawler process
}
```

### Settings

`partitioning` (per Connector):

| Setting | Default | Meaning |
|---|---|---|
| `depth` | 1 | `FileConnector`: split each path into one unit per directory this many levels down, plus one for the files directly in each directory above. `0`: each path is one unit. |
| `maxDirectoriesPerUnit` | unset | A unit lists at most this many directories and hands back the rest. Local paths and S3. |
| `maxUnitSecs` (`partitioning.maxUnitSecs`) | unset | A unit starts no new listing after this long and hands back the rest. Listings already in flight are not cut off. |
| `handBackGroupSize` | `maxDirectoriesPerUnit`, or 64 | How many handed-back directories make one unit. A storage client that walks a group in parallel does well with hundreds. |
| `unitSize` | — | `SequenceConnector`: numbers per unit. |

`sourceRetrySecs` (default 180) and `sourceRetryCapSecs` (default 20), at the Connector's top level, set how long and how often a refused listing is retried.

`crawl` (per run):

| Setting | Default | Meaning |
|---|---|---|
| `workTopic`, `controlTopic` | `lucille_work`, `lucille_control` | Topic names. Created if absent. |
| `workTopicPartitions` | 16 | Partitions to create the work topic with. Changing it later does not change an existing topic. |
| `topicReplicationFactor` | cluster default | For the work, control and event topics. |
| `consumerGroupId` | `lucille_crawlers` | Must differ from `kafka.consumerGroupId`. |
| `threads` | 1 | Units each Crawler process executes at once. |
| `maxOutstandingUnits` | 64 | No more units in flight than this. |
| `maxAttempts` | 3 | Ordinary failures of one unit before the run fails. |
| `maxThrottledAttempts` | 20 | Failures caused by the source refusing, counted apart. |
| `throttleBackoffSecs`, `throttleBackoffCapSecs` | 10, 120 | Wait before a refused unit is dispatched again: between half and all of the first, doubling, up to the cap. |
| `maxSourceConcurrency` | unset | The most concurrent calls the run may make against its source. Only overload refusals (`THROTTLED`, `SOURCE_UNAVAILABLE`) lower the current figure, which also bounds units in flight. |
| `initialSourceConcurrency`, `sourceConcurrencyStep` | a quarter, a tenth of the maximum | Where the figure starts and how much a clean heartbeat adds. |
| `sourceConcurrencyHoldSecs` | `throttleBackoffCapSecs` | After a cut, how long before another. |
| `costsFromRun` | unset | As `-costsFrom`. |
| `heartbeatSecs` | 10 | The Coordinator's heartbeat period. |
| `orphanTimeoutSecs` | 120 | Crawlers give up on a run silent for this long; a Coordinator whose loop is stuck this long exits. |
| `maxUnitSecs` (`crawl.maxUnitSecs`) | unset | Watchdog: a unit executing this long is reported failed (`TIMEOUT`) and its thread replaced. Set it above `partitioning.maxUnitSecs` plus `sourceRetrySecs`. |
| `exitOnTimeout` | false | Exit the Crawler process instead of replacing the thread. |

Numbers may come from the environment; a value substituted from an environment variable is text, and these settings accept a number written that way.

## Sizing

How fast a crawl goes depends on how the work is cut into units more than on how many Crawlers there are. [The measurements]({{< relref "docs/architecture/overview/distributed-crawl#what-the-measurements-show" >}}) on the design page are the evidence for what follows.

- **Concurrency is units in flight.** A unit is executed by one Crawler thread. The number in flight is the smallest of: total Crawler threads, `workTopicPartitions`, `maxOutstandingUnits`, and, with `maxSourceConcurrency` set, the current source-concurrency figure, which starts at `initialSourceConcurrency` (a quarter of the maximum by default) and climbs from there. No unit is dispatched while more Documents are pending than `publisher.maxPendingDocs` allows.
- **Lucille's own S3 client lists on one thread,** so with it each unit makes one call at a time and the bound on units in flight is the run's concurrency against the source. A storage client that lists with a pool multiplies that by its threads; the per-unit share sent in the heartbeat is what caps it.
- **Set `workTopicPartitions` to the number of Crawler threads.** One unit is in flight per partition. With more partitions than threads, a thread reads several and a unit can wait behind another on the same thread; with fewer, some threads have nothing to read.
- **A tree you do not know, or a first crawl: bounded units.** `depth: 0` or `1` with `maxDirectoriesPerUnit` sized so that a unit takes seconds. A few hundred directories is a reasonable start where a listing takes about ten milliseconds. Each unit costs a few milliseconds of messaging (about 8 ms against a broker on the same machine), so thousands of sub-second units are slower than a larger limit. Read the per-unit lines the Coordinator logs: if most units take well under a second, raise the limit; if a few take minutes, lower it or add `partitioning.maxUnitSecs`, which [does not cut off listings in flight]({{< relref "docs/architecture/overview/distributed-crawl#bounded-cut-as-the-tree-is-discovered" >}}). With a client that lists with a pool, set `handBackGroupSize` to a few hundred and make sure the client implements `traverseAll`.
- **Unbounded units only for a tree you know to be even.** A fixed `depth` with no limit has the least overhead, and suits a tree whose subtrees at that depth are of similar size. Choose the smallest `depth` that gives several times more units than Crawler threads, and go one level deeper only if a few units dominate. Where a few subtrees hold most of the work, bounded units were faster even on a recrawl with `-costsFrom` (227 s against 277 s in the [measurements]({{< relref "docs/architecture/overview/distributed-crawl#what-the-measurements-show" >}})).
- **For a recrawl, pass `-costsFrom` with the previous run's ID.** The largest units then start first. This is mainly useful with unbounded or planned units: in a bounded run every unit's cost is capped by the limit, and a handed-back group's key includes a hash of the group, so its cost seldom carries over.
- **A unit's cost is the directories it lists, not the files it finds.** A deep tree of nearly empty directories is slower than a flat one with ten times the files.
- **A distributed crawl can exceed a source's rate limits by a factor of Crawlers × threads.** Retries and backoff keep that from failing the run; `maxSourceConcurrency` keeps it from happening.
- **Add Crawlers until listings per second stop rising.** Past that point more Crawlers only add latency. Where the point lies depends on the source and everything in front of it, so measure it rather than assume it; then set `maxSourceConcurrency` near the requests in flight there.
- **A source that lists fast, such as a local disk,** is often traversed faster by one Connector thread than by any number of Crawlers.

## Restarting a Coordinator automatically

`-resume` is for a person who knows the run stopped. A Coordinator started by an orchestrator, such as a Kubernetes Job that restarts its pod, is started with the same arguments each time and cannot know whether it is the first. Give the run an ID and add `-resumeIfExists`:

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -distributedCrawl -runId nightly-2026-10-05 -resumeIfExists
```

- No record of the run: it is started.
- A record: it is resumed. A run that already completed finds nothing to do and exits successfully.
- A recent heartbeat: the earlier Coordinator may be alive. The new one waits until the heartbeat has been silent for `orphanTimeoutSecs`, then takes over; if it never goes silent, it exits with an error after twice that time. A restart after a crash therefore takes up to `orphanTimeoutSecs` longer than a fresh start.

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -listRuns
```

```
RUN ID                                   STATE                  EPOCH LAST HEARD FROM
nightly-2026-10-04                       ended (complete)           1 86012 secs ago
nightly-2026-10-05                       silent, resumable          1 431 secs ago
```

A run can be resumed for a week after its last heartbeat; after that its record leaves the control topic. A lifecycle method interrupted part way is called again on resume, so `preExecute()` and `postExecute()` should be safe to repeat.

## Reading the Coordinator's log

- One line per unit done: the Crawler, Documents published, source calls, refused calls, duration, parts handed back, and the units outstanding and queued.
- One line per failed unit: its failure class, the count against the relevant limit, and the backoff chosen.
- With `maxSourceConcurrency` set, a line each time the figure changes, and each time the units in flight are re-bounded.
- At the end of each Connector, totals of units, calls refused, and units dispatched again after refusals.

## Security

A distributed crawl adds two shared topics, and what is written to them directs what Crawlers read. Treat write access to them as you would treat the Crawlers' credentials.

- **Restrict who can write to the work and control topics.** Only Coordinators need to write to them; Crawlers only read. Anyone who can write to the control topic can stop any run, by cancelling it or by announcing a higher epoch.
- **A unit cannot widen what a Crawler reads.** A Crawler builds Connectors only from its own config file. The unit supplies a location. `FileConnector` accepts it only if it lies inside the paths it was configured with, is not under a directory in `pathsToSkip`, and is not reached through a symbolic link. `SequenceConnector` accepts only a range inside the configured sequence.
- **Units that do not belong to a live run are discarded.** A unit of a run no Coordinator has announced, of a pipeline the Crawler does not have, with a run ID that could not be part of a topic name, or of a run that was cancelled or taken over, is dropped without a report. A unit of a run whose heartbeat has gone silent is dropped too, but reported `UNIT_FAILED`, so that a Coordinator that is alive but late with its heartbeats dispatches it again.
- **The event topic should be writable only by Lucille's components.** Anyone who can write to it can make a run finish early by reporting units or Documents done; because `FileConnector` expires files no unit saw, an early finish can publish tombstones for files never reached.
- **A unit's failure message is shared.** The exception's class and message, cut to 500 characters, reach the Coordinator's log and the run summary. A Connector whose exceptions quote credentials would expose them.
- **Credentials stay in config.** Units, Events and control records carry none. The config hash on each unit leaves out settings whose names suggest a credential.

## Things to know

- **Document IDs must be stable.** A unit can be executed more than once; random IDs would index a second copy each time.
- **Adding or removing a Crawler can repeat a unit.** When Kafka moves a partition, a unit executing on it is abandoned and executed from the start by the new owner.
- **A dead Crawler's unit waits for Kafka to notice**, 45 seconds by default (`session.timeout.ms`, settable under `kafka.consumer`). A Crawler stopped with SIGINT or SIGTERM takes no new unit, stops reading the control topic, and leaves its consumer group once its current unit ends. With no heartbeats reaching it, a unit still running `orphanTimeoutSecs` after the last one it saw is abandoned as orphaned and executed again elsewhere.
- **Start Crawlers before the run.** A Crawler receives nothing until it has joined its consumer group, which can take tens of seconds when other members have recently come or gone.
- **The event topic is the run's only record.** Replicate it (`topicReplicationFactor`); a run whose event topic is lost cannot be resumed.
- **Without `crawl.maxUnitSecs`, a unit that hangs keeps its Crawler** until `runner.connectorTimeout`.
- **Retries apply in every run mode.** A `FileConnector` that is not distributed also retries refused S3 listings for `sourceRetrySecs` before failing.
- **Handing back needs listings that can be stopped.** The unit limits apply to local paths and S3; other providers walk the whole subtree. One directory holding millions of files is still one unit's work.
- **Changing `partitioning` changes the units.** Resuming with different settings is refused, like any change to a Connector's config.
- **`FileConnector` state needs a shared database.** `state.connectionString` must name a database every Crawler and the Coordinator can reach. Expiry and `sendTombstones` are applied by the Coordinator after all units are done.
- **Crawlers and the Coordinator must share the Connector config.** Each unit carries its hash; a Crawler whose config differs fails the unit rather than crawl something else, and after `maxAttempts` the run fails. Credentials are left out of the hash, so they may differ between machines.
- **The Lucille API runs local runs only.** `RunnerManager` does not start or resume distributed crawls.
