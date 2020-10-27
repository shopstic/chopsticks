package dev.chopsticks.sample.app

import java.util.concurrent.atomic.{AtomicReference, LongAdder}

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.apple.foundationdb.tuple.Versionstamp
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.sample.kvdb.SampleDb.TestValueWithVersionstamp
import dev.chopsticks.sample.kvdb.{SampleDb, SampleDbEnv}
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader
import zio._
import zio.duration._

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
    implicit val testVersionstampValueSerdes: ValueSerdes[TestValueWithVersionstamp] = ValueSerdes.fromKeySerdes

    object default extends SampleDb.Default
    object versionstampKeyTest extends SampleDb.VersionstampKeyTest
    object time extends SampleDb.Time
    object versionstampValueTest extends SampleDb.VersionstampValueTest

    override val keyspacesWithVersionstampKey = Set(
      KeyspaceWithVersionstampKey(versionstampKeyTest)
    )
    override val keyspacesWithVersionstampValue = Set(
      KeyspaceWithVersionstampValue(versionstampValueTest)
    )
  }

  def app = {
    for {
      db <- ZIO.access[SampleDbEnv](_.get)
      watchCounter = new LongAdder()
      changeCounter = new LongAdder()
      lastAtomic = new AtomicReference(Option.empty[TestValueWithVersionstamp])
      dbApi <- KvdbDatabaseApi(db)
      _ <- ZAkkaStreams
        .interruptibleGraph(
          dbApi
            .columnFamily(sampleDb.versionstampValueTest)
            .watchKeySource("foo")
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.foreach { v =>
              lastAtomic.set(v)
              watchCounter.increment()
            })(Keep.both),
          graceful = true
        )
        .log("Watch stream")
        .fork

      fib <- ZIO.effectSuspend {
        changeCounter.increment()
        dbApi.columnFamily(sampleDb.versionstampValueTest).putTask(
          "foo",
          TestValueWithVersionstamp(Versionstamp.incomplete())
        )
      }.repeat(Schedule.forever).fork

      _ <- Task.effectSuspend {
        UIO(println(s"watch=${watchCounter.longValue()} change=${changeCounter.longValue()} last=${lastAtomic.get}"))
      }.repeat(Schedule.fixed(1.second)) raceFirst fib.join
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
          KvdbIoThreadPool.live(),
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
