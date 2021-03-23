#!/usr/bin/env bash
set -euo pipefail

docker build ./shell
IMAGE_ID=$(docker build -q ./shell)

GITHUB_ACTOR=${GITHUB_ACTOR:?"GITHUB_ACTOR env variable is required"}
GITHUB_TOKEN=${GITHUB_TOKEN:?"GITHUB_TOKEN env variable is required"}

docker run \
  -it \
  --rm \
  --privileged \
  --hostname=chopsticks-shell \
  -e GITHUB_TOKEN \
  -e GITHUB_ACTOR \
  -v "${HOME}/Library/Caches/Coursier:/root/.cache/coursier" \
  -v "${HOME}/Library/Caches/com.thesamet.scalapb.protocbridge.protocbridge:/root/.cache/protocbridge" \
  -v "${PWD}:${PWD}" \
  -w "${PWD}" \
  "${IMAGE_ID}" \
  bash -l
