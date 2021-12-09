package dev.chopsticks.sample.app

import akka.stream.scaladsl.{Sink, Source}
import com.apple.foundationdb.tuple.Versionstamp
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp._
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.codec.fdb_key._
import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
import dev.chopsticks.kvdb.codec.protobuf_value._
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool}
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.sample.kvdb.SampleDb.{TestKeyWithVersionstamp, TestValueWithVersionstamp}
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigConvert
import zio._

import scala.concurrent.duration._

final case class FdbTestSampleAppConfig(
  db: FdbDatabase.FdbDatabaseConfig
)

object FdbTestSampleAppConfig {
  // noinspection TypeAnnotation
  implicit val configReader = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[FdbTestSampleAppConfig]
  }
}

object FdbTestSampleApp extends AkkaDiApp[FdbTestSampleAppConfig] {

  object sampleDb extends SampleDb.Materialization {
    implicit val testVersionstampValueSerdes: ValueSerdes[TestValueWithVersionstamp] = ValueSerdes.fromKeySerdes
    implicit val literalVersionstampValueSerdes: ValueSerdes[Versionstamp] = ValueSerdes.fromKeySerdes

    object default extends SampleDb.Default
    object versionstampKeyTest extends SampleDb.VersionstampKeyTest
    object time extends SampleDb.Time
    object versionstampValueTest extends SampleDb.VersionstampValueTest
    object literalVersionstampValueTest extends SampleDb.LiteralVersionstampValueTest

    override val keyspacesWithVersionstampKey = Set(
      KeyspaceWithVersionstampKey(versionstampKeyTest)
    )
    override val keyspacesWithVersionstampValue = Set(
      KeyspaceWithVersionstampValue(versionstampValueTest),
      KeyspaceWithVersionstampValue.literal(literalVersionstampValueTest)
    )
  }

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: FdbTestSampleAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.succeed(appConfig),
          KvdbIoThreadPool.live,
          KvdbSerdesThreadPool.fromDefaultAkkaDispatcher(),
          ZLayer.fromManaged(FdbDatabase.manage(sampleDb, appConfig.db)),
          AppLayer(app)
        )
      )
    }
  }

  override def config(allConfig: Config): Task[FdbTestSampleAppConfig] = Task {
    PureconfigLoader.unsafeLoad[FdbTestSampleAppConfig](allConfig, "app")
  }

  def app: RIO[
    AkkaEnv with IzLogging with MeasuredLogging with Has[SampleDb.Db] with KvdbSerdesThreadPool,
    Unit
  ] = for {
    db <- ZService[SampleDb.Db]
    dbApi <- KvdbDatabaseApi(db)
    _ <- dbApi
      .columnFamily(sampleDb.default)
      .putTask("foo0000", "foo0000")
      .log("put foo0000")
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
      .toZAkkaSource
      .killSwitch
      .interruptibleRunWith(Sink.foreach(println))
      .log("Default dump")

    testKeyspace = dbApi.columnFamily(sampleDb.versionstampKeyTest)

    _ <- testKeyspace
      .drop()
      .log("Drop test")

    _ <- Source(1 to 100)
      .toZAkkaSource
      .viaZAkkaFlow(dbApi.batchTransact(batch => {
        val pairs = batch.zipWithIndex.map {
          case (i, index) =>
            TestKeyWithVersionstamp("foo", i, Versionstamp.incomplete(index)) -> s"foo$i"
        }

        testKeyspace.batchPut(pairs).result -> pairs
      }))
      .killSwitch
      .interruptibleRunIgnore()
      .log("Range populate foo")

    _ <- Source(1 to 100000)
      .toZAkkaSource
      .viaZAkkaFlow(dbApi.batchTransact(batch => {
        val pairs = batch.zipWithIndex.map {
          case (i, index) =>
            TestKeyWithVersionstamp("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
        }

        testKeyspace.batchPut(pairs).result -> pairs
      }))
      .killSwitch
      .interruptibleRunIgnore()
      .log("Range populate bar")

    _ <- testKeyspace
      .source(_ startsWith "bar", _ ltEq "bar" -> 50000)
      .toZAkkaSource
      .killSwitch
      .interruptibleRunWith(Sink.fold(0)((s, _) => s + 1))
      //        .repeat(Schedule.forever)
      .logResult("Range scan", r => s"counted $r")

    lastKey <- testKeyspace
      .getKeyTask(_ lastStartsWith "bar")
      .logResult("Get last test key", _.toString)

    _ <- Source((lastKey.map(_.bar).getOrElse(100000) + 1) to Int.MaxValue)
      .throttle(1, 1.second)
      .toZAkkaSource
      .killSwitch
      .viaZAkkaFlow(
        dbApi.batchTransact(batch => {
          val pairs = batch.zipWithIndex.map {
            case (i, index) =>
              TestKeyWithVersionstamp("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
          }

          testKeyspace.batchPut(pairs).result -> pairs
        })
      )
      .interruptibleRunIgnore()
      .log("Append")
      .fork

    _ <- testKeyspace
      .source(_ ^= "bar", _ ^= "bar")
      .map(_._1)
      .sliding(2)
      .map(_.toList)
      .toZAkkaSource
      .killSwitch
      .interruptibleRunWith(Sink.foreach {
        case previous :: next :: Nil =>
          assert(previous.version.compareTo(next.version) < 0, s"$previous vs $next")
        case _ =>
      })
      .log("Check versionstamp uniqueness")

    _ <- testKeyspace
      .tailSource(_ gt "bar" -> 100000, _ startsWith "bar")
      .toZAkkaSource
      .killSwitch
      .interruptibleRunWith(Sink.foreach {
        case (k, v) =>
          println(s"Tail got: k=$k v=$v")
      })
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
  } yield ()
}
