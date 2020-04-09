package dev.chopsticks.sample.app

import java.time.{LocalDate, LocalDateTime}

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.Config
import dev.chopsticks.fp._
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.berkeleydb_key._
import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
import dev.chopsticks.kvdb.lmdb.LmdbDatabase
import dev.chopsticks.kvdb.util.KvdbClientOptions.Implicits._
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import zio.{Has, RIO, ZLayer}

object KvdbTestSampleApp extends AkkaApp {
  final case class AppConfig(
    db: LmdbDatabase.Config
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
    val dbEnv = LmdbDatabase.manage(sampleDb, appConfig.db).orDie.toLayer

    ZLayer.requires[AkkaApp.Env] ++ configEnv ++ dbEnv
  }

  def run: RIO[Env, Unit] = {
    for {
      db <- ZService[SampleDb.Db]
      dbApi <- KvdbDatabaseApi(db)
      stats <- dbApi.statsTask
      _ <- ZLogger.info(
        stats.toVector
          .sortBy(_._1._1)
          .map(t => s"${t._1._1} (${t._1._2.map(l => s"${l._1}=${l._2}").mkString(" ")}): ${t._2}")
          .mkString("\n")
      )
      defaultCf = dbApi.columnFamily(sampleDb.default)
      tailFiber <- ZAkkaStreams
        .interruptibleGraph(
          for {
            log <- ZService[LogEnv.Service].map(_.logger)
          } yield {
            defaultCf
              .tailSource(_ >= LocalDate.now.getYear.toString, _.last)
              .viaMat(KillSwitches.single)(Keep.right)
              .toMat(Sink.foreach { pair => log.info(s"tail: $pair") })(Keep.both)
          },
          graceful = true
        )
        .fork
      _ <- defaultCf.putTask(LocalDateTime.now.toString, LocalDateTime.now.toString)
      pair <- defaultCf.getTask(_.last)
      _ <- ZLogger.info(s"Got last: $pair")
      _ <- tailFiber.join
    } yield ()
  }
}
