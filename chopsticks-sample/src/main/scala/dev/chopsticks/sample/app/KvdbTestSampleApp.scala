package dev.chopsticks.sample.app

import java.time.{LocalDate, LocalDateTime}

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.Config
import dev.chopsticks.fp._
import dev.chopsticks.kvdb.KvdbDatabase
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.berkeleydb_key._
import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
import dev.chopsticks.kvdb.lmdb.LmdbDatabase
import dev.chopsticks.kvdb.util.KvdbClientOptions.Implicits._
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import zio.{RIO, ZIO, ZManaged}

object KvdbTestSampleApp extends AkkaApp {

  final case class AppConfig(
    db: LmdbDatabase.Config
  )

  type CfgEnv = ConfigEnv[AppConfig]
  type Env = AkkaApp.Env with CfgEnv with SampleDb.Env

  object sampleDb extends SampleDb.Materialization {
    object default extends SampleDb.Default
    object time extends SampleDb.Time
  }

  protected def createEnv(untypedConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env] = {
//    import pureconfig.generic.auto._
    import dev.chopsticks.util.config.PureconfigConverters._

    val envR = for {
      akkaEnv <- ZManaged.environment[AkkaApp.Env]
      typedConfig = PureconfigLoader.unsafeLoad[AppConfig](untypedConfig, "app")
      db <- KvdbDatabase.manage(LmdbDatabase(sampleDb, typedConfig.db))
    } yield new AkkaApp.LiveEnv with CfgEnv with SampleDb.Env {
      val akkaService: AkkaEnv.Service = akkaEnv.akkaService
      val config: AppConfig = typedConfig
      val sampleDb: SampleDb.Db = db
    }

    envR.orDie
  }

  protected def run: RIO[Env, Unit] = {
    val task = for {
      db <- ZIO.access[SampleDb.Env](_.sampleDb)
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
        .interruptableGraph(
          ZIO.access[AkkaEnv with LogEnv] { env =>
            val log = env.logger

            defaultCf
              .tailSource(_ >= LocalDate.now.getYear.toString, _.last)
              .viaMat(KillSwitches.single)(Keep.right)
              .toMat(Sink.foreach { pair =>
                log.info(s"tail: $pair")
              })(Keep.both)
          },
          graceful = true
        )
        .fork
      _ <- defaultCf.putTask(LocalDateTime.now.toString, LocalDateTime.now.toString)
      pair <- defaultCf.getTask(_.last)
      _ <- ZLogger.info(s"Got last: $pair")
      _ <- tailFiber.join
    } yield ()

    task.supervised
  }
}
