package dev.chopsticks.sample.app

import java.time.LocalDateTime

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.Config
import dev.chopsticks.fp.ZIOExt.Implicits._
import dev.chopsticks.fp._
import dev.chopsticks.kvdb.DbFactory.DbClientConfig
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.kvdb.util.DbUtils.Implicits._
import dev.chopsticks.kvdb.{DbClient, DbFactory}
import dev.chopsticks.sample.kvdb.{DummyTestKvdb, DummyTestKvdbEnv}
import dev.chopsticks.util.config.PureconfigLoader
import zio.clock.Clock
import zio.{TaskR, ZIO, ZManaged}

object KvdbTestSampleApp extends AkkaApp {

  final case class AppConfig(
    db: DbClientConfig
  )

  type CfgEnv = ConfigEnv[AppConfig]
  type Env = AkkaApp.Env with CfgEnv with DummyTestKvdbEnv

  protected def createEnv(untypedConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env] = {
//    import pureconfig.generic.auto._
    import dev.chopsticks.util.config.PureconfigConverters._

    val envR = for {
      akkaEnv <- ZManaged.environment[AkkaApp.Env]
      typedConfig = PureconfigLoader.unsafeLoad[AppConfig](untypedConfig, "app")
      db <- createManagedKvdbClient(DummyTestKvdb, typedConfig.db)
    } yield new AkkaApp.LiveEnv with CfgEnv with DummyTestKvdbEnv {
      implicit val actorSystem: ActorSystem = akkaEnv.actorSystem
      val dummyTestKvdb: DbClient[DummyTestKvdb.type] = db
      val config: AppConfig = typedConfig
    }

    envR.orDie
  }

  protected def createManagedKvdbClient[DbDef <: DbDefinition](
    definition: DbDef,
    dbClientConfig: DbClientConfig
  ): ZManaged[Clock with LogEnv with AkkaEnv, Throwable, DbClient[DbDef]] = {
    ZManaged.make {
      for {
        dbClient <- ZIO.access[Clock with LogEnv with AkkaEnv] { implicit env =>
          DbClient[DbDef](DbFactory[DbDef](definition, dbClientConfig))
        }
        _ <- dbClient.open().log("Open db")
      } yield dbClient
    } { dbClient =>
      dbClient.closeTask().log("Close db").catchAll(e => ZLogger.error(s"Failed closing database", e))
    }
  }

  protected def run: TaskR[Env, Unit] = {
    val task = for {
      dbClient <- ZIO.access[DummyTestKvdbEnv](_.dummyTestKvdb)
      defaultCol = dbClient.column(_.Default)
      tailFiber <- ZIOExt
        .interruptableGraph(
          ZIO.access[AkkaEnv with LogEnv] { env =>
            val log = env.logger

            defaultCol
              .tailSource(_.first, _.last)
              .viaMat(KillSwitches.single)(Keep.right)
              .toMat(Sink.foreach { pair =>
                log.info(s"tail: $pair")
              })(Keep.both)
          },
          graceful = true
        )
        .fork
      _ <- defaultCol.putTask(LocalDateTime.now.toString, LocalDateTime.now.toString)
      pair <- defaultCol.getTask(_.last)
      _ <- ZLogger.info(s"Got last: $pair")
      _ <- tailFiber.join
    } yield ()

    task.supervised
  }
}
