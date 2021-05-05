FROM shopstic/bin-dumb-init:1.2.2 as bin-dumb-init
FROM shopstic/bin-jq:1.6.0 as bin-jq
FROM shopstic/curl-tar-unzip:1.0.1 as sbt

ENV SBT_VERSION "1.5.1"

RUN \
  mkdir /sbt && \
  cd /sbt && \
  curl -Lo sbt.zip https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.zip && \
  unzip sbt.zip && rm -Rf sbt.zip && rm -Rf sbt/lib && mv /sbt/sbt/* /sbt/ && rm -Rf sbt && \
  chmod a+x /sbt/bin/sbt

FROM shopstic/curl-tar-unzip:1.0.1 as fdb

ENV FDB_VERSION "6.2.30"

RUN \
  curl -Lko /usr/bin/fdb-clients.deb https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-clients_${FDB_VERSION}-1_amd64.deb && \
  curl -Lko /usr/bin/fdb-server.deb https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-server_${FDB_VERSION}-1_amd64.deb

FROM openjdk:11.0.10-buster as maven

ENV MAVEN_VERSION "3.6.3"

ENV PATH="$PATH:/mvn/bin"

RUN \
  mkdir /mvn && \
  curl -L https://mirror.jframeworks.com/apache/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | \
    tar -xz --strip-components 1 -C /mvn
RUN \
  mvn deploy:help

FROM openjdk:11.0.10-buster

RUN \
  apt update && \
  apt install -y parallel && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/sbt/bin:/mvn/bin"

COPY --from=bin-dumb-init / /
COPY --from=bin-jq / /
COPY --from=sbt /sbt /sbt
COPY --from=maven /mvn /mvn
COPY --from=maven /root/.m2 /root/.m2
COPY --from=fdb /usr/bin/fdb-clients.deb /usr/bin/fdb-clients.deb
COPY --from=fdb /usr/bin/fdb-server.deb /usr/bin/fdb-server.deb

RUN \
  apt-get install /usr/bin/fdb-clients.deb -y && \
  apt-get install /usr/bin/fdb-server.deb -y && \
  rm -f /usr/bin/fdb-clients.deb && \
  rm -f /usr/bin/fdb-server.deb

ARG RUNNER_UID=1001
ARG RUNNER_GID=121

RUN \
  (groupadd --gid "${RUNNER_GID}" runner || true) && \
  useradd --home-dir /home/runner --create-home --uid "${RUNNER_UID}" \
  --gid "${RUNNER_GID}" --shell /bin/bash --skel /dev/null runner

COPY ./.profile /home/runner/.profile
COPY ./settings.xml /home/runner/.m2/settings.xml

RUN \
  chown "${RUNNER_UID}:${RUNNER_GID}" /home/runner/.profile

USER "${RUNNER_UID}:${RUNNER_GID}"

RUN \
  echo "" >> /home/runner/.profile && \
  echo "export PATH=\"$PATH\"" >> /home/runner/.profile

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
