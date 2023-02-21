package dev.chopsticks.sample.app

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.fp.*
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.zio_ext.*
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.schema.config.SchemaConfig
import zio.schema.Schema

import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool}
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.sample.kvdb.SampleDb.{TestKeyWithVersionstamp, TestValueWithVersionstamp}
import zio.Duration._
import zio.*
import zio.stream.*

final case class FdbTestSampleAppConfig(db: FdbDatabase.FdbDatabaseConfig)
object FdbTestSampleAppConfig extends SchemaConfig[FdbTestSampleAppConfig]:
  implicit override lazy val zioSchema: Schema[FdbTestSampleAppConfig] = zio.schema.DeriveSchema.gen

object FdbTestSampleApp extends ZApp {

  object sampleDb extends SampleDb.Materialization {
    override val default: SampleDb.Default = new SampleDb.Default {}
    override val versionstampKeyTest: SampleDb.VersionstampKeyTest = new SampleDb.VersionstampKeyTest {}
    override val time: SampleDb.Time = new SampleDb.Time {}
    override val versionstampValueTest: SampleDb.VersionstampValueTest = new SampleDb.VersionstampValueTest {}
    override val literalVersionstampValueTest: SampleDb.LiteralVersionstampValueTest =
      new SampleDb.LiteralVersionstampValueTest {}

    override val keyspacesWithVersionstampKey =
      Set[KeyspaceWithVersionstampKey[_]](
        KeyspaceWithVersionstampKey(versionstampKeyTest)
      )
    override val keyspacesWithVersionstampValue =
      Set(
        KeyspaceWithVersionstampValue(versionstampValueTest),
        KeyspaceWithVersionstampValue.literal(literalVersionstampValueTest)
      )
  }

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    val dbLayer = ZLayer.fromZIO(
      for
        appConfig <- TypedConfig.get[FdbTestSampleAppConfig]
        db <- FdbDatabase.manage(sampleDb, appConfig.db)
      yield db
    )
    app
      .provideSome[Environment with Scope](
        dbLayer,
        KvdbIoThreadPool.live,
        KvdbSerdesThreadPool.liveFromDefaultBlockingExecutor,
        TypedConfig.live[FdbTestSampleAppConfig]()
      )

  def app: RIO[SampleDb.Db & KvdbSerdesThreadPool, Unit] =
    for
      db <- ZIO.service[SampleDb.Db]
      dbApi <- KvdbDatabaseApi(db)
      _ <- dbApi
        .columnFamily(sampleDb.default)
        .putTask("foo0000", "foo0000")
        .log("put foo0000") @@ ZIOAspect.annotated(
        "the_key_here" -> "foo0000",
        "the_value_here" -> "foo0000",
        "foo" -> true.toString
      )
      _ <- dbApi
        .columnFamily(sampleDb.default)
        .putTask("foo1000", "foo1000")
        .log("put foo1000")
      _ <- dbApi
        .columnFamily(sampleDb.default)
        .getTask(_ is "foo1000")
        .logResult("get", _.toString)

      _ <- dbApi
        .columnFamily(sampleDb.default)
        .source
        .runForeach(tuple => ZIO.succeed(println(tuple)))
        .log("Default dump")

      testKeyspace = dbApi.columnFamily(sampleDb.versionstampKeyTest)

      _ <- testKeyspace
        .drop()
        .log("Drop test")

      _ <- ZStream
        .fromIterable(1 to 100)
        .via {
          dbApi.batchTransact { batch =>
            val pairs = batch.zipWithIndex.map {
              case (i, index) =>
                TestKeyWithVersionstamp("foo", i, Versionstamp.incomplete(index)) -> s"foo$i"
            }
            testKeyspace.batchPut(pairs).result -> pairs
          }
        }
        .runDrain
        .log("Range populate foo")

      _ <- ZStream
        .fromIterable(1 to 10000)
        .via {
          dbApi.batchTransact { batch =>
            val pairs = batch.zipWithIndex.map {
              case (i, index) =>
                TestKeyWithVersionstamp("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
            }
            testKeyspace.batchPut(pairs).result -> pairs
          }
        }
        .runDrain
        .log("Range populate bar")

      _ <- testKeyspace
        .source(_ startsWith "bar", _ ltEq "bar" -> 5000)
        .runFold(0)((s, _) => s + 1)
        //        .repeat(Schedule.forever)
        .logResult("Range scan", r => s"counted $r")

      lastKey <- testKeyspace
        .getKeyTask(_ lastStartsWith "bar")
        .logResult("Get last test key", _.toString)

      _ <- ZStream
        .fromIterable((lastKey.map(_.bar).getOrElse(10000) + 1) to Int.MaxValue)
        .rechunk(1)
        .throttleShape(1, 1.second)(_.size)
        .via(
          dbApi.batchTransact { batch =>
            val pairs = batch.zipWithIndex.map {
              case (i, index) =>
                TestKeyWithVersionstamp("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
            }
            testKeyspace.batchPut(pairs).result -> pairs
          }
        )
        .runDrain
        .log("Append")
        .fork

      _ <- testKeyspace
        .source(_ ^= "bar", _ ^= "bar")
        .map(_._1)
        .sliding(2)
        .map(_.toList)
        .runForeach {
          case previous :: next :: Nil =>
            ZIO.attempt(assert(previous.version.compareTo(next.version) < 0, s"$previous vs $next"))
          case _ =>
            ZIO.unit
        }
        .log("Check versionstamp uniqueness")

      _ <- testKeyspace
        .tailSource(_ gt "bar" -> 10000, _ startsWith "bar")
        .runForeach { case (k, v) =>
          ZIO.succeed(println(s"Tail got: k=$k v=$v"))
        }
        .log("Tail")
    //      stats <- dbApi.statsTask
    //      _ <- ZLogger.info(
    //        stats.toVector
    //          .sortBy(_._1._1)
    //          .map(t => s"${t._1._1} (${t._1._2.map(l => s"${l._1}=${l._2}").mkString(" ")}): ${t._2}")
    //          .mkString("\n")
    //      )
    //      defaultCf = dbApi.columnFamily(sampleDb.default)
    //      tailFiber <- ZAkkaStreams
    //        .interruptibleGraph(
    //          for {
    //            log <- ZService[IzLogging.Service].map(_.logger)
    //          } yield {
    //            defaultCf
    //              .tailSource(_ >= LocalDate.now.getYear.toString, _.last)
    //              .viaMat(KillSwitches.single)(Keep.right)
    //              .toMat(Sink.foreach { pair => log.info(s"tail: $pair") })(Keep.both)
    //          },
    //          graceful = true
    //        )
    //        .fork
    //      _ <- defaultCf.putTask(LocalDateTime.now.toString, LocalDateTime.now.toString)
    //      pair <- defaultCf.getTask(_.last)
    //      _ <- ZLogger.info(s"Got last: $pair")
    //      _ <- tailFiber.join
    yield ()

}
