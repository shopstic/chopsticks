#!/usr/bin/env bash
set -euo pipefail

ci_build_in_shell() {
  local GITHUB_REF=${GITHUB_REF:?"GITHUB_REF env variable is required"}
  local GITHUB_SHA=${GITHUB_SHA:?"GITHUB_SHA env variable is required"}
  local GITHUB_TOKEN=${GITHUB_TOKEN:?"GITHUB_TOKEN env variable is required"}
  local GITHUB_WORKSPACE=${GITHUB_WORKSPACE:?"GITHUB_WORKSPACE env variable is required"}

  cat <<EOF | docker run \
    --workdir /repo \
    -i \
    --rm \
    -e SBT_OPTS="-Xmx3g -Xss6m" \
    -e "GITHUB_SHA=${GITHUB_SHA}" \
    -e "GITHUB_TOKEN=${GITHUB_TOKEN}" \
    -v "${GITHUB_WORKSPACE}:/repo" \
    -v "${HOME}/.cache:/root/.cache" \
    "${IMAGE_NAME}:${IMAGE_TAG}" \
    bash

set -euo pipefail

export FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
service foundationdb start

./cli.sh ci_build

if [[ "${GITHUB_REF}" == "refs/heads/main" ]]; then
  ./cli.sh ci_publish
fi

EOF

}

ci_build() {
  sbt --client 'set ThisBuild / scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:**", "-opt:l:method", "-Werror")'
  sbt --client show ThisBuild / scalacOptions | tail -n4
  sbt --client cq
  sbt --client compile
  sbt --client printWarnings
  sbt --client test
}

ci_publish() {
  local GITHUB_SHA=${GITHUB_SHA:?"GITHUB_SHA env variable is required"}

  local CURRENT_VERSION
  CURRENT_VERSION=$(sbt --client show version | grep SNAPSHOT | head -n1 | awk '{print $2}' | sed s/-SNAPSHOT//)

  local TIMESTAMP
  TIMESTAMP=$(date -u +"%Y%m%d%H%M%S")

  local SHORTENED_COMMIT_SHA
  SHORTENED_COMMIT_SHA=$(echo "${GITHUB_SHA}" | cut -c 1-7)

  local PUBLISH_VERSION="${CURRENT_VERSION}-${TIMESTAMP}-${SHORTENED_COMMIT_SHA}"

  echo "Current version is ${CURRENT_VERSION}"
  echo "Publish version is ${PUBLISH_VERSION}"

  sbt --client "set ThisBuild / version := \"${PUBLISH_VERSION}\""
  sbt --client publish
}

publish_fdb_jar() {
  VERSION=${1:?"Version is required"}
  SHOPSTIC_BINTRAY_API_KEY=${SHOPSTIC_BINTRAY_API_KEY:?"SHOPSTIC_BINTRAY_API_KEY env variable is required"}

  TEMP=$(mktemp -d)
  trap "rm -Rf ${TEMP}" EXIT

  cd "${TEMP}"

  wget "https://www.foundationdb.org/downloads/6.2.19/bindings/java/fdb-java-${VERSION}.jar"
  wget "https://www.foundationdb.org/downloads/6.2.19/bindings/java/fdb-java-${VERSION}-javadoc.jar"

  mkdir fdb
  wget -O - https://github.com/apple/foundationdb/archive/6.2.19.tar.gz | tar -xz --strip-components=1 -C ./fdb
  jar cf "fdb-java-${VERSION}-sources.jar" -C ./fdb/bindings/java/src/main .

  cat << EOF > "fdb-java-${VERSION}.pom"
<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>org.foundationdb</groupId>
  <artifactId>fdb-java</artifactId>
  <version>${VERSION}</version>
  <packaging>jar</packaging>

  <name>foundationdb-java</name>
  <description>Java bindings for the FoundationDB database. These bindings require the FoundationDB client, which is under a different license. The client can be obtained from https://www.foundationdb.org/download/.</description>
  <inceptionYear>2010</inceptionYear>
  <url>https://www.foundationdb.org</url>

  <organization>
    <name>FoundationDB</name>
    <url>https://www.foundationdb.org</url>
  </organization>

  <developers>
    <developer>
        <name>FoundationDB</name>
    </developer>
  </developers>

  <scm>
    <url>http://0.0.0.0</url>
  </scm>

  <licenses>
    <license>
      <name>The Apache v2 License</name>
      <url>http://www.apache.org/licenses/</url>
    </license>
  </licenses>

</project>
EOF

  FILES=("fdb-java-${VERSION}.jar" "fdb-java-${VERSION}-javadoc.jar" "fdb-java-${VERSION}-sources.jar" "fdb-java-${VERSION}.pom")

  for FILE in "${FILES[@]}" ; do
    echo ""
    echo "Uploading ${FILE}..."
    curl \
      -T "${FILE}" \
      "-ushopstic:${SHOPSTIC_BINTRAY_API_KEY}" \
      "https://api.bintray.com/content/shopstic/maven/org.foundationdb:fdb-java/${VERSION}/org/foundationdb/fdb-java/${VERSION}/${FILE}"
  done

  echo "---------------------------"
  echo "Publishing..."
  curl \
    -X POST \
    "-ushopstic:${SHOPSTIC_BINTRAY_API_KEY}" \
    "https://api.bintray.com/content/shopstic/maven/org.foundationdb:fdb-java/${VERSION}/publish"
}

loc() {
  find . -type f \( -name "*.scala" -o -name "*.sbt" -o -name "*.proto" -o -name "*.conf" \) -not -path "./*/target/*" | xargs wc -l | awk '{total += $1} END{print total}'
}

sbt_shell() {
  if ! ls ./project/target/active.json > /dev/null 2>&1; then
    sbt -Dsbt.semanticdb=true
  else
    sbt --client
  fi
}

sbt_shutdown() {
  if ls ./project/target/active.json > /dev/null 2>&1; then
    sbt --client shutdown
  fi
}


"$@"
