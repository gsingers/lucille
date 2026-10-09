#!/bin/bash
# Runs the whole distributed crawl in one JVM, with in-memory queues in place of Kafka.
# Run from the example's directory after `mvn package`:  ./scripts/run-in-process.sh
set -e
cd "$(dirname "$0")/.."
CP='target/classes:target/lib/*'

[ -d target/sample-tree ] || java -cp "$CP" com.kmwllc.lucille.example.crawl.SampleTree target/sample-tree
rm -f target/crawl-output.csv

java -Dconfig.file=conf/crawl.conf -cp "$CP" com.kmwllc.lucille.example.crawl.InProcessDemo

echo
echo "Documents written: $(($(wc -l < target/crawl-output.csv) - 1)) (see target/crawl-output.csv)"
