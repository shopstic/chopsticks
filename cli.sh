#!/usr/bin/env bash
set -euo pipefail

export GITHUB_ORG="shopstic"
export GITHUB_PROJECT_NAME="chopsticks"
export ARTIFACT_ORG="dev.chopsticks"

export LOCAL_PUBLISH_PATH="${HOME}/.ivy2/local/${ARTIFACT_ORG}"

ci_run_in_shell() {
  local GITHUB_SHA=${GITHUB_SHA:?"GITHUB_SHA env variable is required"}
  local GITHUB_REF=${GITHUB_REF:?"GITHUB_REF env variable is required"}
  local GITHUB_ACTOR=${GITHUB_ACTOR:?"GITHUB_ACTOR env variable is required"}
  local GITHUB_TOKEN=${GITHUB_TOKEN:?"GITHUB_TOKEN env variable is required"}
  local GITHUB_WORKSPACE=${GITHUB_WORKSPACE:?"GITHUB_WORKSPACE env variable is required"}
  local SHELL_IMAGE=${SHELL_IMAGE:?"SHELL_IMAGE env variable is required"}

  docker run \
    --workdir /repo \
    -i \
    --rm \
    -e "GITHUB_SHA=${GITHUB_SHA}" \
    -e "GITHUB_ACTOR=${GITHUB_ACTOR}" \
    -e "GITHUB_TOKEN=${GITHUB_TOKEN}" \
    -e "GITHUB_REF=${GITHUB_REF}" \
    -v "${GITHUB_WORKSPACE}:/repo" \
    -v "${HOME}/.cache:/home/runner/.cache" \
    -v "${HOME}/.sbt:/home/runner/.sbt" \
    "${SHELL_IMAGE}" \
    bash -c "./cli.sh ci_run"
}

ci_run() {
  local GITHUB_REF=${GITHUB_REF:?"GITHUB_REF env variable is required"}

  mkdir -p /etc/foundationdb/
  export FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
  echo "docker:docker@127.0.0.1:4500" > "${FDB_CLUSTER_FILE}"

  export SBT_OPTS="-server -XX:+UseG1GC -Xms6g -Xmx6g -Xss6m"
  ./cli.sh build

  if [[ "${GITHUB_REF}" == "refs/heads/master" ]]; then
    local PUBLISH_VERSION
    PUBLISH_VERSION=$(./cli.sh get_publish_version)

    ./cli.sh publish_local "${PUBLISH_VERSION}"

    sbt --client shutdown # To reduce occupied memory for subsequent processes

    ./cli.sh publish_remote "${PUBLISH_VERSION}"
  fi
}

build() {
  sbt --client 'set ThisBuild / scalacOptions ++= Seq("-Werror")'
  sbt --client show ThisBuild / scalacOptions | tail -n4
  sbt --client cq
  sbt --client compile
  sbt --client Test / compile
  sbt --client printWarnings
  sbt --client Test / printWarnings
  sbt --client 'set ThisBuild / Test / fork := false'
  sbt --client test
}

get_publish_version() {
  local GITHUB_SHA=${GITHUB_SHA:?"GITHUB_SHA env variable is required"}

  local CURRENT_VERSION
  CURRENT_VERSION=$(sbt --client show version | grep "\[info\]" | tail -n1 | awk '{print $2}')

  if [[ "${CURRENT_VERSION}" == *"-SNAPSHOT" ]]; then
    CURRENT_VERSION=${CURRENT_VERSION/-SNAPSHOT/}

    local TIMESTAMP
    TIMESTAMP=$(date -u +"%Y%m%d%H%M%S")

    local SHORTENED_COMMIT_SHA
    SHORTENED_COMMIT_SHA=$(echo "${GITHUB_SHA}" | cut -c 1-7)

    echo "${CURRENT_VERSION}-${TIMESTAMP}-${SHORTENED_COMMIT_SHA}"
  else
    echo "${CURRENT_VERSION}"
  fi
}

publish_local() {
  local PUBLISH_VERSION=${1:?"Publish version is required"}

  echo "Publishing to local with version ${PUBLISH_VERSION}"

  sbt --client "set ThisBuild / version := \"${PUBLISH_VERSION}\""
  sbt --client publishLocal
}

publish_remote() {
  local PUBLISH_VERSION=${1:?"Publish version is required"}

  echo "Publishing to remote with version ${PUBLISH_VERSION}"

  local DEPLOY_CMD_TEMPLATE
  DEPLOY_CMD_TEMPLATE=$(
    cat <<EOF
set -euo pipefail
echo "Publishing %MODULE%"
mvn deploy:deploy-file \
	-DpomFile=${LOCAL_PUBLISH_PATH}/%MODULE%/${PUBLISH_VERSION}/poms/%MODULE%.pom \
 	-Dfile=${LOCAL_PUBLISH_PATH}/%MODULE%/${PUBLISH_VERSION}/jars/%MODULE%.jar \
 	-Dsources=${LOCAL_PUBLISH_PATH}/%MODULE%/${PUBLISH_VERSION}/srcs/%MODULE%-sources.jar \
  	-DrepositoryId=github \
  	-Durl=https://maven.pkg.github.com/${GITHUB_ORG}/${GITHUB_PROJECT_NAME}
EOF
  )

  # shellcheck disable=SC2038
 find "${LOCAL_PUBLISH_PATH}" -mindepth 1 -maxdepth 1 -type d |
    xargs -I{} basename {} |
    parallel -j12 --progress --halt=now,fail=1 --retries=2 -I"%MODULE%" "${DEPLOY_CMD_TEMPLATE}"
}

unpublish_debug_packages() {
  local GITHUB_ACTOR=${GITHUB_ACTOR:?"GITHUB_ACTOR env variable is required"}
  local GITHUB_TOKEN=${GITHUB_TOKEN:?"GITHUB_TOKEN env variable is required"}

  cat << EOF > ~/.netrc
machine api.github.com
login ${GITHUB_ACTOR}
password ${GITHUB_TOKEN}
EOF

  chmod 0400 ~/.netrc

  local GITHUB_PACKAGE_CLEANUP_CMD
  GITHUB_PACKAGE_CLEANUP_CMD=$(cat <<EOF
set -euo pipefail
PACKAGE_NAME="${ARTIFACT_ORG}.%MODULE%"

DELETE_PACKAGE_VERSION_CMD=\$(cat <<EOC
set -euo pipefail
echo "Deleting package \${PACKAGE_NAME} at version %VERSION%"
curl -snf -X DELETE -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/orgs/${GITHUB_ORG}/packages/maven/\${PACKAGE_NAME}/versions/%VERSION%
EOC
)

echo "Fetching all versions for \${PACKAGE_NAME}"
curl -snf -H "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/orgs/${GITHUB_ORG}/packages/maven/\${PACKAGE_NAME}/versions" |
  jq -r '.[] | select(.name | contains("debug")) | .id' |
  parallel -j12 --halt=now,fail=1 -I"%VERSION%" "\${DELETE_PACKAGE_VERSION_CMD}"
EOF
)

  # shellcheck disable=SC2038
 find "${LOCAL_PUBLISH_PATH}" -mindepth 1 -maxdepth 1 -type d |
    xargs -I{} basename {} |
    parallel -j12 --halt=now,fail=1 -I"%MODULE%" "${GITHUB_PACKAGE_CLEANUP_CMD}"
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

  cat <<EOF >"fdb-java-${VERSION}.pom"
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

  for FILE in "${FILES[@]}"; do
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
  if ! ls ./project/target/active.json >/dev/null 2>&1; then
    sbt -Dsbt.semanticdb=true
  else
    sbt --client
  fi
}

sbt_shutdown() {
  if ls ./project/target/active.json >/dev/null 2>&1; then
    sbt --client shutdown
  fi
}

"$@"
