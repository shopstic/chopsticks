package dev.chopsticks.sample.app

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.fp._
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.fdb_key._
import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
import dev.chopsticks.kvdb.codec.protobuf_value._
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.sample.kvdb.SampleDb.TestKey
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import zio.{Has, RIO, UIO, ZLayer}

import scala.concurrent.duration._

object FdbTestSampleApp extends AkkaApp {
  final case class AppConfig(
    db: FdbDatabase.FdbDatabaseConfig
  )

  type Env = AkkaApp.Env with Has[AppConfig] with SampleDb.Env

  object sampleDb extends SampleDb.Materialization {
    object default extends SampleDb.Default
    object test extends SampleDb.Test
    object time extends SampleDb.Time
  }

  protected def createEnv(untypedConfig: Config): ZLayer[AkkaApp.Env, Nothing, Env] = {
    import dev.chopsticks.util.config.PureconfigConverters._

    val appConfig = PureconfigLoader.unsafeLoad[AppConfig](untypedConfig, "app")
    val configEnv = ZLayer.succeed(appConfig)
    val dbEnv = FdbDatabase.manage(sampleDb, appConfig.db).orDie.toLayer

    ZLayer.requires[AkkaApp.Env] ++ configEnv ++ dbEnv
  }

  def run: RIO[Env, Unit] = {
    for {
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

      _ <- ZAkkaStreams
        .graph {
          Source(1 to 100)
            .map(i => TestKey("foo", i) -> s"foo$i")
            .via(dbApi.columnFamily(sampleDb.test).putInBatchesFlow)
            .toMat(Sink.ignore)(Keep.right)
        }
        .log("Range populate foo")

      _ <- ZAkkaStreams
        .graph(
          Source(1 to 100000)
            .map(i => TestKey("bar", i) -> s"bar$i")
            .via(dbApi.columnFamily(sampleDb.test).putInBatchesFlow)
            .toMat(Sink.ignore)(Keep.right)
        )
        .log("Range populate bar")

      _ <- ZAkkaStreams
        .interruptibleGraph(
          dbApi
            .columnFamily(sampleDb.test)
            .source(_ startsWith "bar", _ ltEq TestKey("bar", 50000))
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
              lastKey <- dbApi
                .columnFamily(sampleDb.test)
                .getKeyTask(_ lastStartsWith "bar")
                .logResult("Get last test key", _.toString)
              source <- UIO(
                Source((lastKey.map(_.bar).getOrElse(100000) + 1) to Int.MaxValue)
                  .throttle(1, 1.second)
                  .map(i => TestKey("bar", i) -> s"bar$i")
                  .viaMat(KillSwitches.single)(Keep.right)
                  .via(
                    dbApi
                      .columnFamily(sampleDb.test)
                      .putInBatchesFlow
                  )
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
          dbApi
            .columnFamily(sampleDb.test)
            .tailSource(_ startsWith "bar", _ startsWith "bar")
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
}
