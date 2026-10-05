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
| `FileConnector` | A directory (local) or key prefix (S3) to traverse recursively, or the files directly inside one | `partitioning { depth: 1, maxDirectoriesPerUnit: 500 }` |
| `SequenceConnector` | A range of the sequence | `partitioning { unitSize: 1000 }` |

For `FileConnector`, `depth` is how many levels below each configured path the split happens. With `depth: 1` and a path containing thirty directories, the plan is thirty-one units: one per directory, and one for the files that sit directly in the path.

### Units that turn out to be large

A plan made at a fixed depth knows nothing about what lies beneath it. One directory may hold most of the tree, and the unit for it would be walked by a single Crawler while the others sit idle. A unit can therefore **hand part of itself back**.

For `FileConnector`, set `partitioning.maxDirectoriesPerUnit`, `partitioning.maxUnitSecs`, or both. A unit lists directories until it reaches either limit. It still publishes every file in the directories it has listed, but the directories it has found and not listed are handed back, in groups, and each group becomes a unit, to be executed by whichever Crawler receives it. Those units are limited in the same way, so a subtree of any size is cut into pieces no larger than the limit, and nearly every piece is as large as the limit allows: a tree of 4,000 directories with a limit of 50 becomes about 90 units. A unit always lists at least the directory it was given.

With these limits `depth` matters much less, and may be `0`: each configured path is then planned as one unit, and the tree is shared out as it is discovered. Without them, a unit walks all of its subtree, as before.

The Crawler does not dispatch the parts itself. It names them in its report, and the Coordinator creates the units, so that the Coordinator remains the one place that knows every unit of the run and can tell when all are done. A part is handed back only if its unit completes: a unit that fails, or whose Crawler dies, is executed again from the start. A part that is already a unit of the run is not created twice.

A Connector of your own hands work back by calling `UnitContext.handBack(key, payload)` from `executeUnit`. The same part of the source must always be given the same key.

Any other Connector, or one of these without a `partitioning` block, still works in a distributed crawl. It runs as a single unit, on one Crawler, by calling its ordinary `execute()` method. Its work is not parallelized, but it no longer runs inside the Runner.

Units are small JSON records. Their content is a description of *where* to read, never the Connector class or its settings: a Crawler only executes Connectors that are named in its own config, and a `FileConnector` refuses a unit whose path lies outside the paths it was configured with.

## Knowing When the Run Is Complete

In every other mode, the Runner knows a Connector is finished because it published every Document itself and received an Event when each was indexed. In a distributed crawl the Coordinator publishes nothing, so it learns of everything through Events on the run's event topic:

- A Crawler sends a `CREATE` Event for each Document, once Kafka has accepted the Document. The Coordinator tracks the Document from then until the Indexer reports it, exactly as it already tracks child Documents that a pipeline creates.
- A Crawler sends `UNIT_DONE` when all of a unit's Documents and their `CREATE` Events have been accepted, or `UNIT_FAILED` if the unit threw an exception. If the unit handed parts back, `UNIT_CHILDREN` Events naming them come first, and `UNIT_DONE` says how many to expect. A unit whose parts did not all arrive is treated as failed.

A Connector is complete when planning has finished, no unit is outstanding or waiting to be dispatched, and no Document is pending.

The Coordinator applies back-pressure by holding units back. It dispatches no more while `crawl.maxOutstandingUnits` units are incomplete, or while more Documents are pending than `publisher.maxPendingDocs` allows. Units that were handed back wait in the Coordinator's memory until there is room, a few hundred bytes each.

As each unit finishes, the Coordinator logs the Crawler that executed it, the Documents it published, the calls it made to the source (for `FileConnector`, directories listed), how long it took and how many parts it handed back.

## When Things Fail

Delivery is at-least-once throughout, as it already is from the source topic onward. Every recovery described below can cause some Documents to be published twice. They carry the same IDs both times, so the search engine ends up with one copy, but the run summary counts each copy that was indexed.

**A unit throws an exception.** The Crawler reports `UNIT_FAILED` and the Coordinator dispatches the unit again, up to `crawl.maxAttempts` times in total. After that the Connector fails, and with it the run.

**A unit hangs.** Nothing is done about it unless `crawl.maxUnitSecs` is set. If it is, a unit that has been executing for that long is reported as failed, which counts against `crawl.maxAttempts`, and is dispatched again. A thread that is blocked cannot be stopped, so the Crawler leaves it behind, gives up that thread's partitions of the work topic so that nothing waits behind it, and starts a new thread in its place. With `crawl.exitOnTimeout: true` the Crawler process exits instead, which suits a deployment that restarts it and is the only way to free what the stuck thread holds. Set the limit well above the longest a healthy unit takes; `partitioning.maxUnitSecs` and `maxDirectoriesPerUnit` are what keep units short.

**A Crawler dies.** A Crawler claims a unit by reading it from the work topic and commits its position only when the unit is finished. The unit of a Crawler that dies is therefore delivered to another Crawler, with no action from the Coordinator.

**The Coordinator dies.** The Coordinator sends a heartbeat every `crawl.heartbeatSecs`. Crawlers stop working on a run whose heartbeat has been silent for `crawl.orphanTimeoutSecs`, so an abandoned run does not occupy them. The run can then be resumed:

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -resume <runId>
```

Everything the Coordinator decides is written to the run's event topic before it takes effect, so the new Coordinator rebuilds its state by reading that topic from the beginning. Connectors that had completed are skipped, lifecycle methods that had returned are not called again, and units that were done are not executed again. Units that were outstanding are dispatched again.

Each Coordinator of a run has an **epoch**, one higher than the last, which it stamps on its units and heartbeats. Crawlers discard units from an older epoch than the newest they have heard from. This is what makes it safe to dispatch the outstanding units again without knowing what became of the earlier copies, and it means a Coordinator that was wrongly presumed dead cannot keep work alive once another has taken over. Such a Coordinator notices the newer epoch at its next heartbeat and stops, reporting its run as failed. `-resume` refuses to take over a run whose heartbeat is recent unless `-force` is given.

**The Coordinator is alive but stuck.** Heartbeats are sent by a thread of their own, so a Coordinator whose main thread had stopped reading Events would otherwise go on announcing a run that can no longer finish, and a replacement would be refused. The heartbeat thread checks: if the main thread has not gone round its loop for `crawl.orphanTimeoutSecs`, the Coordinator stops its heartbeat and exits with a failure. It does not cancel the run, which is then in the same state as one whose Coordinator died, and is resumed the same way. Only the wait for units and Documents is watched; a Connector's own lifecycle methods may take as long as they need.

A run can be resumed for a week after its last heartbeat. After that its record is removed from the control topic.

### Restarting a Coordinator automatically

`-resume` is for a person who knows the run stopped. A Coordinator that is restarted by something else, such as a Kubernetes Job, is started with the same arguments each time and cannot know whether it is the first. For that, give the run an ID and add `-resumeIfExists`:

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -distributedCrawl -runId nightly-2026-10-05 -resumeIfExists
```

- If there is no record of the run, it is started.
- If there is, it is resumed. A run that already completed is resumed too, finds nothing left to do, and exits successfully.
- If the run's last heartbeat is recent, its earlier Coordinator may still be alive. The new one waits until the heartbeat has been silent for `crawl.orphanTimeoutSecs` and then takes over. If the heartbeat does not go silent, another Coordinator has the run, and the new one exits with an error after twice that time.

A restart therefore takes up to `crawl.orphanTimeoutSecs` longer than the process took to come back.

To see which runs are on record, and which can be resumed:

```bash
java -Dconfig.file=<CONFIG> -cp '...' com.kmwllc.lucille.core.Runner -listRuns
```

```
RUN ID                                   STATE                  EPOCH LAST HEARD FROM
nightly-2026-10-04                       ended (complete)           1 86012 secs ago
nightly-2026-10-05                       silent, resumable          1 431 secs ago
```

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
| `-resumeIfExists` | With `-distributedCrawl` and `-runId`, start the run if it is new and continue it if it is not. For a Runner that is restarted automatically. |
| `-listRuns` | List the distributed crawls on record, with the state of each, and exit. |

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

How fast a crawl goes is decided by how the work is cut into units, more than by how many Crawlers there are.

- **Concurrency is the number of units in flight.** A unit is executed by one Crawler thread, which walks it alone. The number in flight is the smallest of: the total Crawler threads, `crawl.workTopicPartitions`, and `crawl.maxOutstandingUnits`.
- **The largest unit is a floor.** No number of Crawlers finishes a run faster than its longest unit takes. Crawlers beyond `total work / largest unit` add nothing. `partitioning.maxDirectoriesPerUnit` puts a ceiling on the largest unit, whatever the shape of the tree.
- **A unit's cost is the number of directories it lists, not the number of files it finds.** A deep tree of nearly empty directories is slower than a flat one holding ten times the files.
- **Each unit costs several milliseconds of messaging on top of its own work** (about 8 ms against a broker on the same machine): two flushes, a report, a commit and the next poll. Units should take seconds or more. A `depth` that yields thousands of units of a few hundred files each makes a crawl slower than not distributing it at all.
- **Units cannot be moved once dealt.** They are dealt to the work topic's partitions in turn, and a partition is read by one Crawler thread, one unit at a time. A Crawler that has finished its own units does not take those queued behind another's long unit. Keeping units small is what keeps that wait short.
- **Set `crawl.workTopicPartitions` to the number of Crawler threads you intend to run,** or a small multiple of it. The topic is created with that many partitions the first time any component needs it; changing the setting later does not change an existing topic.
- **Crawlers have their own consumer group.** `crawl.consumerGroupId` must differ from `kafka.consumerGroupId`, or every Crawler that joins or leaves would interrupt the Workers. A config in which they are the same is rejected.

In practice, for a tree whose shape you do not know: use `depth: 0` or `1` with `maxDirectoriesPerUnit` set so that a unit takes seconds, not milliseconds. A few hundred directories is a reasonable start for a source where a listing takes around ten milliseconds. Then read the per-unit lines the Coordinator logs: if most units take well under a second, raise the limit; if a few take minutes, lower it or add `maxUnitSecs`.

For a tree you do know, a fixed `depth` with no limit has the least overhead: choose the smallest `depth` that gives several times more units than Crawler threads, and go one level deeper only if a few units dominate.

A source that is fast to list, such as a local disk, is often traversed faster by a single connector thread than by any number of Crawlers. Distributing the crawl pays when listing the source is slow and the source can serve many listings at once.

## Security

A distributed crawl adds two shared topics, and what is written to them directs what Crawlers read. Treat write access to them as you would treat access to the Crawlers' credentials.

- **Restrict who can write to the work and control topics.** With Kafka ACLs, only Coordinators need to write to `crawl.workTopic` and `crawl.controlTopic`; Crawlers only read them. Anyone who can write to the control topic can stop any run, by cancelling it or by announcing a higher epoch for it. The run's Coordinator reports the run as failed when that happens.
- **A unit cannot widen what a Crawler reads.** A Crawler builds Connectors only from its own config file. The unit supplies a location. `FileConnector` accepts it only if it lies inside the paths that Connector was configured with, is not under a directory in `pathsToSkip`, and is not reached through a symbolic link. `SequenceConnector` accepts only a range inside the configured sequence.
- **Units that do not belong to a live run are discarded.** That covers a unit whose run no Coordinator has announced, one that names a pipeline the Crawler does not have, and one whose run ID could not be part of a topic name. A Crawler reports nothing about such a unit, so it cannot be made to write to a topic of someone else's choosing.
- **A run's event topic is as sensitive as it already was.** Anyone who can write to it can make a run finish early, in any mode, by reporting Documents or units as done. Because `FileConnector` expires files that no unit reported seeing, a run that is made to finish early can publish tombstones for files that were never reached. The event topic should be writable only by Lucille's own components.
- **A unit's failure message is shared.** When a unit fails, the exception's class and message, cut to 500 characters, are sent to the Coordinator and appear in its log and in the run summary. A Connector whose exceptions quote credentials would expose them there.
- **Credentials stay in config.** Units, Events and control records carry no credentials. The config hash carried by each unit leaves out any setting whose name suggests a credential.
- **Run IDs** given with `-runId` may contain letters, digits, `.`, `_` and `-`, up to 128 characters.

## Things to Know

- **Document IDs must be stable.** A unit can be executed more than once. A Connector that generates random IDs would index a second copy each time.
- **Adding or removing a Crawler can repeat a unit.** When Crawlers join or leave, Kafka may move a partition from one Crawler to another. A unit that was executing on it is abandoned by the first Crawler and executed from the start by the second.
- **A dead Crawler's unit waits for Kafka to notice.** A Crawler that is killed outright is removed from its consumer group only when its session times out, 45 seconds by default (`session.timeout.ms`, settable under `kafka.consumer`). Its unit is delivered to another Crawler after that. A Crawler that is stopped with SIGINT or SIGTERM finishes its unit and leaves the group at once.
- **A run's topics are replicated as the cluster is configured to.** The work topic, the control topic and each run's event topic are created with the cluster's default replication factor unless `crawl.topicReplicationFactor` is set. The event topic is the only record of what a run has done, so a run whose event topic is lost cannot be resumed. In the other run modes the event topic is always created with one replica.
- **Numbers in the `crawl` block may come from the environment.** A value substituted from an environment variable is text; `crawl.*` and `partitioning.*` settings accept a number written that way.
- **Start Crawlers before the run.** A Crawler receives nothing until it has joined its consumer group, which can take tens of seconds when other members have recently come or gone.
- **A unit that hangs is not detected by default.** Without `crawl.maxUnitSecs`, a Crawler that is alive but stuck on a unit keeps it, and the run waits until `runner.connectorTimeout`.
- **Handing back needs listings that can be stopped.** `maxDirectoriesPerUnit` and `maxUnitSecs` apply to local paths and S3. For other providers a unit walks all of its subtree. The limits count directories, so one directory holding millions of files is still one unit's work.
- **Changing the limits changes the units.** Resuming a run with different `partitioning` settings is refused, like any other change to a Connector's config.
- **`FileConnector` state needs a shared database.** A partitioned `FileConnector` with `state` enabled requires `state.connectionString` to name a database that every Crawler and the Coordinator can reach; the embedded default is a file in one process's working directory. Expiry and `sendTombstones` are applied by the Coordinator after all units are done, since no single unit sees every file.
- **Crawlers and the Coordinator must have the same Connector config.** Each unit carries a hash of the Connector's config, and a Crawler whose own config hashes differently fails the unit rather than crawl something other than what was planned. Credentials are left out of the hash, so they may differ between machines.
- **The Lucille API runs local runs only.** `RunnerManager` does not start or resume distributed crawls.
