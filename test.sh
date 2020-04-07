#!/usr/bin/env bash
set -euo pipefail

for i in {1..100}; do
  echo "Test round: ${i}"
  sbt "stream / testOnly *ZAkkaStreamsTest"
done
