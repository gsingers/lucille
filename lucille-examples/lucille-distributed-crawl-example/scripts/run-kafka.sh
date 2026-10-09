#!/bin/bash
# Runs the distributed crawl as separate processes talking through Kafka: two Crawlers, a Worker, an Indexer, and
# a Coordinator. Needs a Kafka broker at localhost:9092 (or KAFKA_BOOTSTRAP_SERVERS), for example:
#
#   docker run -d --name crawl-example-kafka -p 9092:9092 apache/kafka:4.2.0
#
# Run from the example's directory after `mvn package`:  ./scripts/run-kafka.sh
# Logs go to target/logs/.
set -e
cd "$(dirname "$0")/.."
CP='target/classes:target/lib/*'
CONF=conf/kafka.conf
mkdir -p target/logs

[ -d target/sample-tree ] || java -cp "$CP" com.kmwllc.lucille.example.crawl.SampleTree target/sample-tree
rm -f target/crawl-output.csv

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

# The long-lived roles first. A Crawler takes nothing until it has joined its consumer group.
for i in 1 2; do
  java -Dconfig.file=$CONF -cp "$CP" com.kmwllc.lucille.core.Crawler > target/logs/crawler$i.log 2>&1 &
  pids+=($!)
done
java -Dconfig.file=$CONF -cp "$CP" com.kmwllc.lucille.core.Worker crawl_pipeline > target/logs/worker.log 2>&1 &
pids+=($!)
java -Dconfig.file=$CONF -cp "$CP" com.kmwllc.lucille.core.Indexer crawl_pipeline > target/logs/indexer.log 2>&1 &
pids+=($!)

echo "Waiting for the Crawlers to join their consumer group..."
for _ in $(seq 1 60); do
  joined=$(grep -l "Successfully synced group" target/logs/crawler*.log 2>/dev/null | wc -l | tr -d ' ')
  [ "$joined" -ge 2 ] && break
  sleep 1
done

# The Coordinator: one per run. Its log shows each unit as it is done, by which Crawler, and what it handed back.
run_id="example-$(date +%Y%m%d-%H%M%S)"
java -Dconfig.file=$CONF -cp "$CP" com.kmwllc.lucille.core.Runner -distributedCrawl -runId "$run_id" \
  | tee target/logs/coordinator.log | grep --line-buffered -E "done by|RUN SUMMARY|complete:" || true

echo
echo "Units done: $(grep -c 'done by' target/logs/coordinator.log); of which handed parts back: $(grep -Ec ' [1-9][0-9]* handed back' target/logs/coordinator.log || true)"
echo "Documents written: $(($(wc -l < target/crawl-output.csv) - 1)) (see target/crawl-output.csv)"
echo "To see the units spread over the Crawlers:  grep 'done by' target/logs/coordinator.log | sed -E 's/.*done by (Crawler-[a-z0-9]+-[0-9]+).*/\1/' | sort | uniq -c"
