package dev.chopsticks.sample.app

import java.time.Instant
import java.util.concurrent.atomic.LongAdder

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.Config
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.{AkkaDiApp, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.stream.ZAkkaStreams
import zio.clock.Clock
import zio.{RIO, Schedule, Task, UIO, ZIO, ZLayer}
import zio.duration._
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader

object FdbWatchTestApp extends AkkaDiApp {
  override type Env = AkkaEnv with LogEnv with Clock with AppConfig with SampleDb.Env

  final case class FdbWatchTestAppConfig(
    db: FdbDatabase.FdbDatabaseConfig
  )

  object FdbWatchTestAppConfig {
    //noinspection TypeAnnotation
    implicit val configReader = {
      import dev.chopsticks.util.config.PureconfigConverters._
      ConfigReader[FdbWatchTestAppConfig]
    }
  }

  object sampleDb extends SampleDb.Materialization {
    import dev.chopsticks.kvdb.codec.fdb_key._
    import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
    import dev.chopsticks.kvdb.codec.protobuf_value._
    object default extends SampleDb.Default
    object test extends SampleDb.Test
    object time extends SampleDb.Time
  }

  override type Cfg = FdbWatchTestAppConfig

  override def app: RIO[Env, Unit] = {
    for {
      db <- ZIO.access[SampleDb.Env](_.get)
      watchCounter = new LongAdder()
      changeCounter = new LongAdder()
      dbApi <- KvdbDatabaseApi(db)
      _ <- ZAkkaStreams
        .interruptibleGraph(
          dbApi
            .columnFamily(sampleDb.default)
            .watchKeySource("foo")
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.foreach { _ =>
              watchCounter.increment()
            })(Keep.both),
          graceful = true
        )
        .log("Watch stream")
        .fork

      _ <- Task.effectSuspend {
        changeCounter.increment()
        dbApi.columnFamily(sampleDb.default).putTask("foo", Instant.now.toString)
      }.repeat(Schedule.forever).fork

      _ <- Task.effectSuspend {
        UIO(println(s"watch=${watchCounter.longValue()} change=${changeCounter.longValue()}"))
      }.repeat(Schedule.fixed(1.second))
    } yield ()
  }

  override def liveEnv(akkaAppDi: DiModule, appConfig: Cfg, allConfig: Config): Task[DiEnv[Env]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.fromManaged(FdbDatabase.manage(sampleDb, appConfig.db)),
          ZLayer.succeed(appConfig),
          ZIO.environment[Env]
        )
      )
    }
  }

  override def config(allConfig: Config): Task[Cfg] = Task(PureconfigLoader.unsafeLoad[Cfg](allConfig, "app"))
}
