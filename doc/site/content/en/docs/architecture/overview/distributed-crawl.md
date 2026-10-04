---
title: "Distributing the Crawl"
weight: 5
date: 2026-10-04
description: >
  How a run can spread a Connector's work over many Crawler processes, how the run still knows when it is complete, and what happens when a Crawler or the Runner fails.
---

The [previous page]({{< relref "docs/architecture/overview/local-vs-distributed" >}}) showed how Workers and Indexers can run as separate processes that communicate through Kafka. In that arrangement one thing still happens in a single place: each Connector executes on one thread, inside the Runner. For a source that is slow to read, such as a large bucket whose every object has to be fetched, that thread limits the whole run no matter how many Workers are waiting.

A **distributed crawl** removes the limit. The Runner no longer executes Connectors. It splits each Connector's work into **work units**, and separately deployed **Crawler** processes execute them.

```
                  work topic                    source topic                dest topic
 Runner ──units──▶ ┌───────┐ ──▶ Crawler ──docs──▶ ┌───────┐ ──▶ Worker ──▶ ┌───────┐ ──▶ Indexer
 (Coordinator)     └───────┘ ──▶ Crawler ──docs──▶ └───────┘ ──▶ Worker ──▶ └───────┘
      ▲                      ──▶ Crawler                                          │
      │                             │                                             │
      └──────────────── events: units done, documents created and indexed ────────┘
```

Workers and Indexers are unchanged. They cannot tell whether a Document was published by a Runner or by a Crawler.

## The Three Roles

| Role | Started with | What it does |
|---|---|---|
| **Coordinator** | `Runner -distributedCrawl` | Owns the run. For each Connector in turn: calls its lifecycle methods, has it plan its work units, dispatches the units, and waits until every unit is done and every Document is indexed. Produces the run summary. |
| **Crawler** | `com.kmwllc.lucille.core.Crawler` | Stays alive across runs. Receives a unit, builds the Connector the unit names from its own copy of the config, has it publish the unit's Documents, and reports the outcome. |
| **Worker / Indexer** | as in distributed mode | Unchanged. |

A run is still a sequence of Connectors, each one finishing before the next begins, and the run still stops if a Connector fails.

## Work Units

A Connector that can be split implements `PartitionableConnector` and is given a `partitioning` block in the config:

| Connector | A unit is | Config |
|---|---|---|
| `FileConnector` | A directory (local) or key prefix (S3) to traverse recursively, or the files directly inside one | `partitioning { depth: 1 }` |
| `SequenceConnector` | A range of the sequence | `partitioning { unitSize: 1000 }` |

For `FileConnector`, `depth` is how many levels below each configured path the split happens. With `depth: 1` and a path containing thirty directories, the plan is thirty-one units: one per directory, and one for the files that sit directly in the path.

Any other Connector, or one of these without a `partitioning` block, still works in a distributed crawl. It runs as a single unit, on one Crawler, by calling its ordinary `execute()` method. Its work is not parallelized, but it no longer runs inside the Runner.

Units are small JSON records. Their content is a description of *where* to read, never the Connector class or its settings: a Crawler only executes Connectors that are named in its own config, and a `FileConnector` refuses a unit whose path lies outside the paths it was configured with.

## Knowing When the Run Is Complete

In every other mode, the Runner knows a Connector is finished because it published every Document itself and received an Event when each was indexed. In a distributed crawl the Coordinator publishes nothing, so it learns of everything through Events on the run's event topic:

- A Crawler sends a `CREATE` Event for each Document, once Kafka has accepted the Document. The Coordinator tracks the Document from then until the Indexer reports it, exactly as it already tracks child Documents that a pipeline creates.
- A Crawler sends `UNIT_DONE` when all of a unit's Documents and their `CREATE` Events have been accepted, or `UNIT_FAILED` if the unit threw an exception.

A Connector is complete when planning has finished, no unit is outstanding, and no Document is pending.

The Coordinator applies back-pressure by holding units back. It dispatches no more while `crawl.maxOutstandingUnits` units are incomplete, or while more Documents are pending than `publisher.maxPendingDocs` allows.

## When Things Fail

Delivery is at-least-once throughout, as it already is from the source topic onward. Every recovery described below can cause some Documents to be published twice. They carry the same IDs both times, so the search engine ends up with one copy, but the run summary counts each copy that was indexed.

**A unit throws an exception.** The Crawler reports `UNIT_FAILED` and the Coordinator dispatches the unit again, up to `crawl.maxAttempts` times in total. After that the Connector fails, and with it the run.

**A Crawler dies.** A Crawler claims a unit by reading it from the work topic and commits its position only when the unit is finished. The unit of a Crawler that dies is therefore delivered to another Crawler, with no action from the Coordinator.

**The Coordinator dies.** The Coordinator sends a heartbeat every `crawl.heartbeatSecs`. Crawlers stop working on a run whose heartbeat has been silent for `crawl.orphanTimeoutSecs`, so an abandoned run does not occupy them. The run can then be resumed:

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -resume <runId>
```

Everything the Coordinator decides is written to the run's event topic before it takes effect, so the new Coordinator rebuilds its state by reading that topic from the beginning. Connectors that had completed are skipped, lifecycle methods that had returned are not called again, and units that were done are not executed again. Units that were outstanding are dispatched again.

Each Coordinator of a run has an **epoch**, one higher than the last, which it stamps on its units and heartbeats. Crawlers discard units from an older epoch than the newest they have heard from. This is what makes it safe to dispatch the outstanding units again without knowing what became of the earlier copies, and it means a Coordinator that was wrongly presumed dead cannot keep work alive once another has taken over. `-resume` refuses to take over a run whose heartbeat is recent unless `-force` is given.

A lifecycle method that was interrupted part way is called again by the resuming Coordinator, so `preExecute()` and `postExecute()` should be safe to repeat.

## Running a Distributed Crawl

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
| `-runId <id>` | Use this run ID instead of generating one. Pass one when the Runner is restarted automatically, so the restart can `-resume` it. Works in every mode. |
| `-resume <id>` | Continue the unfinished distributed crawl with this run ID. |
| `-force` | With `-resume`, take over even if the run's heartbeat is recent. |

A minimal config adds a `partitioning` block to the Connector. The `crawl` block is optional; see `application-example.conf` for every setting and its default.

```hocon
connectors: [
  {
    name: "docs"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "pipeline1"
    paths: ["s3://my-bucket/docs/"]
    s3 { region: "us-east-1" }
    partitioning { depth: 1 }
  }
]

crawl {
  workTopicPartitions: 16   # at most this many units execute at once, across all Crawlers
  threads: 4                # units executed at once by each Crawler process
}
```

### Sizing

- **Partitions bound parallelism.** Each partition of the work topic is read by one Crawler thread at a time, so no more than `crawl.workTopicPartitions` units execute at once. The topic is created with that many partitions the first time any component needs it; changing the setting later does not change an existing topic.
- **Units should take minutes, not hours.** A unit is the smallest thing that can be retried or moved to another Crawler. Choose a `depth` that yields many more units than Crawler threads, so that one unusually large directory does not leave the rest idle.
- **Crawlers have their own consumer group.** `crawl.consumerGroupId` must differ from `kafka.consumerGroupId`. Sharing a group would make every Crawler that joins or leaves interrupt the Workers.

## Things to Know

- **Document IDs must be stable.** A unit can be executed more than once. A Connector that generates random IDs would index a second copy each time.
- **Adding or removing a Crawler can repeat a unit.** When Crawlers join or leave, Kafka may move a partition from one Crawler to another. A unit that was executing on it is abandoned by the first Crawler and executed from the start by the second.
- **A unit that hangs is not detected.** A Crawler that is alive but stuck on a unit keeps it. The run then waits until `runner.connectorTimeout`.
- **`FileConnector` state needs a shared database.** A partitioned `FileConnector` with `state` enabled requires `state.connectionString` to name a database that every Crawler and the Coordinator can reach; the embedded default is a file in one process's working directory. Expiry and `sendTombstones` are applied by the Coordinator after all units are done, since no single unit sees every file.
- **Crawlers and the Coordinator must have the same Connector config.** Each unit carries a hash of the Connector's config, and a Crawler whose own config hashes differently fails the unit rather than crawl something other than what was planned. Credentials are left out of the hash, so they may differ between machines.
- **The Lucille API runs local runs only.** `RunnerManager` does not start or resume distributed crawls.
