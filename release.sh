#!/usr/bin/env bash
set -euo pipefail

get_release_version() {
  local VERSION_SBT
  VERSION_SBT=$(cat ./version.sbt) || exit $?

  local REGEX='ThisBuild / version := "([^"]+)"'

  if [[ $VERSION_SBT =~ $REGEX ]]; then
    local CURRENT_VERSION="${BASH_REMATCH[1]}"

    if [[ $CURRENT_VERSION =~ -SNAPSHOT$ ]]; then
      CURRENT_VERSION=${CURRENT_VERSION/-SNAPSHOT/}
      local GIT_SHA
      local GIT_COMMIT_COUNT
      local SHORTENED_COMMIT_SHA

      GIT_SHA=$(git rev-parse HEAD) || exit $?
      SHORTENED_COMMIT_SHA=$(echo "${GIT_SHA}" | cut -c 1-7) || exit $?
      GIT_COMMIT_COUNT=$(git rev-list --count HEAD) || exit $?

      echo "${CURRENT_VERSION}+${GIT_COMMIT_COUNT}-${SHORTENED_COMMIT_SHA}-SNAPSHOT"
    else
      echo "${CURRENT_VERSION}"
    fi
  else
    echo >&2 "Cannot determine version from version.sbt"
    exit 1
  fi
}

release() {
  local SONATYPE_USERNAME=${SONATYPE_USERNAME:?"SONATYPE_USERNAME is required"}
  local SONATYPE_PASSWORD=${SONATYPE_PASSWORD:?"SONATYPE_PASSWORD is required"}
  local PGP_SIGNING_KEY=${PGP_SIGNING_KEY:?"PGP_SIGNING_KEY is required"}
  local RELEASE_VERSION
  RELEASE_VERSION=$("$0" get_release_version) || exit $?

  sbt --client "set ThisBuild / version := \"${RELEASE_VERSION}\""
  sbt --client 'set ThisBuild / publishTo := sonatypePublishToBundle.value'
  sbt --client 'set ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"'
  sbt --client "set ThisBuild / credentials += Credentials(\"Sonatype Nexus Repository Manager\", \"s01.oss.sonatype.org\", \"${SONATYPE_USERNAME}\", \"${SONATYPE_PASSWORD}\")"
  sbt --client "set Global / pgpSigningKey := Some(\"${PGP_SIGNING_KEY}\")"
  sbt --client sonatypeBundleClean
  sbt --client publishSigned

  if [[ ! $RELEASE_VERSION =~ -SNAPSHOT$ ]]; then
    sbt --client sonatypeBundleRelease
  fi
}

"$@"
