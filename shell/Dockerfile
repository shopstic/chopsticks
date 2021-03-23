FROM shopstic/curl-tar-unzip:1.0.1 as dumb-init

ENV DUMB_INIT_VERSION "1.2.2"

RUN \
  curl -Lo /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v${DUMB_INIT_VERSION}/dumb-init_${DUMB_INIT_VERSION}_amd64 && \
  chmod +x /usr/bin/dumb-init

FROM openjdk:11.0.10-buster as sbt

ENV SBT_VERSION "1.4.9"
ENV SCALA_VERSION=2.13.5

ENV PATH="$PATH:/sbt/bin"

RUN \
  mkdir /sbt && \
  cd /sbt && \
  curl -Lo sbt.zip https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.zip && \
  unzip sbt.zip && rm -Rf sbt.zip && rm -Rf sbt/lib && mv /sbt/sbt/* /sbt/ && rm -Rf sbt && \
  chmod a+x /sbt/bin/sbt

RUN \
  mkdir ~/temp && \
  cd ~/temp && \
  mkdir -p ./src/main/scala && \
  touch ./src/main/scala/temp.scala && \
  sbt "set scalaVersion := \"${SCALA_VERSION}\"" compile && \
  rm -Rf ~/temp

FROM shopstic/curl-tar-unzip:1.0.1 as fdb

ENV FDB_VERSION "6.2.29"

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

FROM shopstic/curl-tar-unzip:1.0.1 as jq

ENV JQ_VERSION "1.6"

RUN \
  curl -Lo /usr/bin/jq https://github.com/stedolan/jq/releases/download/jq-${JQ_VERSION}/jq-linux64 && \
  chmod +x /usr/bin/jq

FROM openjdk:11.0.10-buster

RUN \
  apt update && \
  apt install -y parallel && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/sbt/bin:/mvn/bin"

COPY --from=dumb-init /usr/bin/dumb-init /usr/bin/dumb-init
COPY --from=sbt /sbt /sbt
COPY --from=sbt /root/.sbt /root/.sbt
COPY --from=maven /mvn /mvn
COPY --from=maven /root/.m2 /root/.m2
COPY --from=fdb /usr/bin/fdb-clients.deb /usr/bin/fdb-clients.deb
COPY --from=fdb /usr/bin/fdb-server.deb /usr/bin/fdb-server.deb
COPY --from=jq /usr/bin/jq /usr/bin/jq

RUN \
  apt-get install /usr/bin/fdb-clients.deb -y && \
  apt-get install /usr/bin/fdb-server.deb -y && \
  rm -f /usr/bin/fdb-clients.deb && \
  rm -f /usr/bin/fdb-server.deb

COPY ./.profile /root/.profile
COPY ./settings.xml /root/.m2/settings.xml

RUN \
  echo "" >> /root/.profile && \
  echo "export PATH=\"$PATH\"" >> /root/.profile

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
