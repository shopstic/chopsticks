#!/usr/bin/env bash

find . -type f \( -name "*.scala" -o -name "*.sbt" -o -name "*.proto" -o -name "*.conf" \) -not -path "./*/target/*" | xargs wc -l | awk '{total += $1} END{print total}'
