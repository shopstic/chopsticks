package dev.chopsticks.sample.app

import java.time.Instant
import java.util.concurrent.atomic.LongAdder

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.sample.kvdb.{SampleDb, SampleDbEnv}
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader
import zio.duration._
import zio._

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

object FdbWatchTestApp extends AkkaDiApp[FdbWatchTestAppConfig] {

  object sampleDb extends SampleDb.Materialization {
    import dev.chopsticks.kvdb.codec.fdb_key._
    import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
    import dev.chopsticks.kvdb.codec.protobuf_value._
    object default extends SampleDb.Default
    object test extends SampleDb.Test
    object time extends SampleDb.Time

    override val keyspacesWithVersionstamp = Set.empty
  }

  def app: RIO[AkkaEnv with LogEnv with MeasuredLogging with SampleDbEnv, Unit] = {
    for {
      db <- ZIO.access[SampleDbEnv](_.get)
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

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: FdbWatchTestAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.fromManaged(FdbDatabase.manage(sampleDb, appConfig.db)),
          ZLayer.succeed(appConfig),
          AppLayer(app)
        )
      )
    }
  }

  override def config(allConfig: Config): Task[FdbWatchTestAppConfig] = Task(
    PureconfigLoader.unsafeLoad[FdbWatchTestAppConfig](allConfig, "app")
  )
}
