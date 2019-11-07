# Scala chopsticks

`chopsticks` is an essential collection of Scala libraries for everyday use. It's designed specifically to build high performance, production-grade functional reactive stream systems. These have been used extensively in our internal production streaming systems for years, dealing with __over 1 million persistent writes / s with concurrently up to 30 million reads / s__ in each individual server.

## License

[Apache 2.0 license](./LICENSE.txt)

## Download [ ![Download](https://api.bintray.com/packages/shopstic/maven/chopsticks-fp/images/download.svg) ](https://bintray.com/shopstic/maven/chopsticks-fp/_latestVersion)

All artifacts are published to [bintray](https://bintray.com/shopstic/). Simply add a resolver to your `build.sbt`:

```scala
ThisBuild / resolvers += Resolver.bintrayRepo("shopstic", "maven")
```

## chopsticks-kvdb-*

[Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html) integration with 2 of the world's best open-source, embedded, ordered key-value storage engines: [LMDB](http://www.lmdb.tech/doc/) and [RocksDB](https://github.com/facebook/rocksdb), with a clear abstraction layer to integrate with more engines in the future.

To use, add the corresponding modules to your `build.sbt`. For example:

```scala
libraryDependencies ++= Seq(
  "dev.chopsticks" %% "chopsticks-kvdb-codec-berkeleydb-key" % CHOPSTICKS_VERSION,
  "dev.chopsticks" %% "chopsticks-kvdb-codec-protobuf-value" % CHOPSTICKS_VERSION,
  "dev.chopsticks" %% "chopsticks-kvdb-rocksdb" % CHOPSTICKS_VERSION
)
```

Embedded, ordered key-value databases are essential components of modern end-to-end reactive stream, distributed, microservice-driven systems, where local persistent states need to be maintained at extremely high throughput and low latency. To achieve the same result using shared distributed databases, one would need a substantially larger cluster consisting of many dozens of nodes.

However, embedded KV databases are far from being easy to use, since they are very low level by design. They are meant to power higher-level databases / applications, and only deal with raw arrays of bytes for both keys and values, leaving serialization / deserialization completely up to the application. They also share the same calling threads with the application itself, which requires careful thread management and allocation.

`chopsticks-kvdb` abstracts all of the hard parts away behind Akka Streams interfaces, which makes utilizing these databases significantly simpler:

- Auto derivation of key serializer / deserializer for case classes and most of the common JVM data types. 
- Key serializers ensure keys are lexicographically ordered, leveraging the excellent Tuple implementation from [Berkeley DB](https://en.wikipedia.org/wiki/Berkeley_DB).
- Auto derivation of key prefixes which guarantees type-safety of prefixes at compile time. [Prefix range scan](https://github.com/facebook/rocksdb/wiki/rocksdb-basics#prefix-iterators) is fundamental to ordered KV databases.
- Supports any value serialization format, out-of-the-box integration with [ScalaPB](https://github.com/scalapb/ScalaPB)
- High-level, easy to use iterator key constraint DSL
- 100% async, non-blocking, with auto IO-bound thread management for all database operations.
- First-class Akka Streams APIs, which allow you to simply treat the database as a collection of stream sources.
- Flexible configuration to trade off between throughput vs. latency.
- Multiple storage engine backends behind the same APIs, which enable freedom to choose the right one for the right job (BTree or LSM).

## chopsticks-fp

```scala
libraryDependencies ++= Seq(
  "dev.chopsticks" %% "chopsticks-fp" % CHOPSTICKS_VERSION
)
```

Basic building blocks for [Akka-based](https://akka.io/) applications with [ZIO](https://zio.dev/docs/getting_started.html) integration, focusing on functional programming while retaining the ability to utilize the vast ecosystem of Akka for enterprise applications.

The most commonly used piece is the [AkkaApp](./chopsticks-fp/src/main/scala/dev/chopsticks/fp/AkkaApp.scala) trait. As a simple template:

```scala
object MyAppAkka extends AkkaApp {
  type Env = AkkaApp.Env

  protected def createEnv(untypedConfig: Config) = ZManaged.environment[AkkaApp.Env]

  def run: ZIO[Env, Throwable, Unit] = {
    for {
      akkaService <- ZIO.access[LogEnv](_.akkaService)
      // ...
    } yield ()
  }
}
```

An `ActorSystem` is automatically managed together with the life-cycle of the application. The default ZIO `Platform` env is configured with the Akka's dispatcher thread pool. Additionally, the app's [Lightbend config](https://github.com/lightbend/config) is automatically loaded from classpath. The config file name is derived from the app's class name, by converting a *PascalCase* `MyAwesomeApp` class name to *kebab-case* `my-awesome-app.conf` file name. 

## chopsticks-stream

```scala
libraryDependencies ++= Seq(
  "dev.chopsticks" %% "chopsticks-stream" % CHOPSTICKS_VERSION
)
```

A collection of very useful [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html) extensions. Most notably:

### [BatchWithOptionalAggregateFlow](./chopsticks-stream/src/main/scala/dev/chopsticks/stream/BatchWithOptionalAggregateFlow.scala)

Similar to [batch](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/batch.html), but allows the aggregate function to decide whether to force emitting the currently aggregated buffer based on its accumulated state, vs. just based on the cost function.

As an example, we can batch upstream elements while downstream is slower as usual, but emit the accumulated buffer right away if there's another element already contained in the UUID Set. This ensures every batch consists of non-overlapping transactions against the same set of entities.

```scala
Flow[Transaction]
  .batchWithOptionalAggregate(1024, tx => {
    val idSet = mutable.Set.empty[UUID] += tx.rowId
    val buf = List.newBuilder[Transaction] += tx
    (idSet, buf)
  }) {
    case ((idSet, buf), tx) =>
      val uuid = tx.rowId
      if (idSet.contains(uuid)) None
      else Some((idSet += uuid, buf += tx))
  }
  .map(_._2.result())
```

### [ChildProcessFlow](./chopsticks-stream/src/main/scala/dev/chopsticks/stream/ChildProcessFlow.scala)

Enables stream processing with forked child processes, in a completely async, non-blocking manner. This is incredibly useful to interop with other legacy systems.

```scala
Source
  .single(ByteString.empty) // stdin
  .viaMat(ChildProcessFlow(...))(Keep.right)
  .map {
    case Out(bs, Stdout) => ... // stdout
    case Out(bs, StdErr) => ... // stderr
  }
```

### [MultiMergeSorted](./chopsticks-stream/src/main/scala/dev/chopsticks/stream/MultiMergeSorted.scala)

Similar to [mergeSorted](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mergeSorted.html), but supports any arbitrary number of sources to merge, instead of just 2.

This is indispensable when you need to combine multiple concurrent time-series sources into a single time-series, ordered in a single timeline. The ordering criteria can be supplied via an `Ordering` implicit value.

```scala
implicit val timeOrdering: Ordering[Event] = ...
val sources: List[Source[Event, NotUsed]] = List(ts1, ts2, ts3, ts4)
MultiMergeSorted
  .merge(sources, untilLastSourceComplete = true)
```

### [StatefulMapConcatWithCompleteFlow](./chopsticks-stream/src/main/scala/dev/chopsticks/stream/StatefulMapConcatWithCompleteFlow.scala)

Similar to [statefulMapConcat](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/statefulMapConcat.html), but enables emitting the last accumulated state when the stream completes.

### [ZAkkaStreams](./chopsticks-stream/src/main/scala/dev/chopsticks/stream/ZAkkaStreams.scala)

Akka Streams married to ZIO, which allows utilization of ZIO's effect instead of Future. Supported stream ops are:

- `effectMapAsync`: equivalent to [mapAsync](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapAsync.html#mapasync)
- `effectMapAsyncUnordered`: equivalent to [mapAsyncUnordered](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapAsyncUnordered.html#mapasyncunordered)
- `interruptibleEffectMapAsync`: similar to `effectMapAsync` but will interrupt the pending effects when the stream completes. This guarantees no leaks, which is a common problem with mid-stream Futures.
- `interruptibleEffectMapAsyncUnordered`
- `switchFlatMapConcat`: Akka Streams does not have an equivalence of Rx's [switchMap](https://www.learnrxjs.io/operators/transformation/switchmap.html), which enables cancellation of the outstanding sub-stream and switching to the new one. This fills in the gap nicely.
 
To use those, simply import:

```scala
import dev.chopsticks.stream.ZAkkaStreams.ops._
```

