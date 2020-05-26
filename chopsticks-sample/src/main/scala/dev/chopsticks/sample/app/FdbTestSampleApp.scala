package dev.chopsticks.sample.app

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.apple.foundationdb.tuple.Versionstamp
import com.typesafe.config.Config
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp._
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.fdb_key._
import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
import dev.chopsticks.kvdb.codec.protobuf_value._
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.KeyspaceWithVersionstamp
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.sample.kvdb.SampleDb.TestKey
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader
import zio._

import scala.concurrent.duration._

object FdbTestSampleApp extends AkkaDiApp {
  final case class Cfg(
    db: FdbDatabase.FdbDatabaseConfig
  )

  object Cfg {
    //noinspection TypeAnnotation
    implicit val configReader = {
      import dev.chopsticks.util.config.PureconfigConverters._
      ConfigReader[Cfg]
    }
  }

  type Env = AkkaApp.Env with AppConfig with SampleDb.Env

  object sampleDb extends SampleDb.Materialization {
    object default extends SampleDb.Default
    object test extends SampleDb.Test
    object time extends SampleDb.Time

    override val keyspacesWithVersionstamp = Set(
      KeyspaceWithVersionstamp(test)
    )
  }

  override def liveEnv(akkaAppDi: DiModule, appConfig: Cfg, allConfig: Config): Task[DiEnv[Env]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.succeed(appConfig),
          ZLayer.fromManaged(FdbDatabase.manage(sampleDb, appConfig.db)),
          ZIO.environment[Env]
        )
      )
    }
  }

  override def config(allConfig: Config): Task[Cfg] = Task {
    PureconfigLoader.unsafeLoad[Cfg](allConfig, "app")
  }

  override def app: RIO[Env, Unit] = for {
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

    _ <- ZAkkaStreams
      .graph(
        dbApi
          .columnFamily(sampleDb.default)
          .source
          .toMat(Sink.foreach(println))(Keep.right)
      )
      .log("Default dump")

    testKeyspace = dbApi.columnFamily(sampleDb.test)

    _ <- testKeyspace
      .drop()
      .log("Drop test")

    _ <- ZAkkaStreams
      .graph {
        Source(1 to 100)
          .via(dbApi.batchTransact((tx, batch) => {
            testKeyspace.batchPut(
              tx,
              batch.zipWithIndex.map {
                case (i, index) =>
                  TestKey("foo", i, Versionstamp.incomplete(index)) -> s"foo$i"
              }
            )
          }))
          .toMat(Sink.ignore)(Keep.right)
      }
      .log("Range populate foo")

    _ <- ZAkkaStreams
      .graph(
        Source(1 to 100000)
          .via(dbApi.batchTransact((tx, batch) => {
            testKeyspace.batchPut(
              tx,
              batch.zipWithIndex.map {
                case (i, index) =>
                  TestKey("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
              }
            )
          }))
          .toMat(Sink.ignore)(Keep.right)
      )
      .log("Range populate bar")

    _ <- ZAkkaStreams
      .interruptibleGraph(
        testKeyspace
          .source(_ startsWith "bar", _ ltEq "bar" -> 50000)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.fold(0)((s, _) => s + 1))(Keep.both),
        graceful = true
      )
      //        .repeat(Schedule.forever)
      .logResult("Range scan", r => s"counted $r")

    _ <- ZAkkaStreams
      .interruptibleGraphM(
        {
          for {
            lastKey <- testKeyspace
              .getKeyTask(_ lastStartsWith "bar")
              .logResult("Get last test key", _.toString)
            source <- UIO(
              Source((lastKey.map(_.bar).getOrElse(100000) + 1) to Int.MaxValue)
                .throttle(1, 1.second)
                .viaMat(KillSwitches.single)(Keep.right)
                .via(dbApi.batchTransact((tx, batch) => {
                  testKeyspace.batchPut(
                    tx,
                    batch.zipWithIndex.map {
                      case (i, index) =>
                        TestKey("bar", i, Versionstamp.incomplete(index)) -> s"bar$i"
                    }
                  )
                }))
                .toMat(Sink.ignore)(Keep.both)
            )
          } yield source
        },
        graceful = true
      )
      .log("Append")
      .fork

    _ <- ZAkkaStreams
      .interruptibleGraph(
        testKeyspace
          .source(_ ^= "bar", _ ^= "bar")
          .map(_._1)
          .sliding(2)
          .map(_.toList)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.foreach {
            case one :: two :: Nil =>
              assert(one.version.compareTo(two.version) < 0, s"$one vs $two")
            case _ =>
          })(Keep.both),
        graceful = true
      )
      .log("Check versionstamp uniqueness")

    _ <- ZAkkaStreams
      .interruptibleGraph(
        testKeyspace
          .tailSource(_ gt "bar" -> 100000, _ startsWith "bar")
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.foreach {
            case (k, v) =>
              println(s"Tail got: k=$k v=$v")
          })(Keep.both),
        graceful = true
      )
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
//            log <- ZService[LogEnv.Service].map(_.logger)
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
