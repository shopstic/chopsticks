package dev.chopsticks.sample.app

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink}
import com.apple.foundationdb.tuple.Versionstamp
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.sample.kvdb.SampleDb.TestValueWithVersionstamp
import dev.chopsticks.sample.kvdb.{SampleDb, SampleDbEnv}
import dev.chopsticks.stream.ZAkkaGraph.InterruptibleGraphOps
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader
import zio._
import zio.duration._

import java.util.concurrent.atomic.{AtomicReference, LongAdder}

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
      actorSystem <- AkkaEnv.actorSystem
      _ <- LoggedRace()
        .add(
          "logging",
          Task
            .effectSuspend {
              UIO(
                println(s"watch=${watchCounter.longValue()} change=${changeCounter.longValue()} last=${lastAtomic.get}")
              )
            }
            .repeat(Schedule.fixed(1.second.asJava))
            .unit
        )
        .add(
          "watch",
          dbApi
            .columnFamily(sampleDb.versionstampValueTest)
            .watchKeySource("foo")
            .viaMat(KillSwitches.single)(Keep.both)
            .toMat(Sink.foreach { v =>
              lastAtomic.set(v)
              watchCounter.increment()
            }) { case ((f1, ks), f2) =>
              import actorSystem.dispatcher
              ks -> f1.flatMap(_ => f2)
            }
            .interruptibleRun()
            .unit
            .onInterrupt(UIO(println("GONNA HANG ON FOR A WHILE")).delay(java.time.Duration.ofSeconds(5)))
        )
        .add(
          "update",
          ZIO
            .effectSuspend {
              changeCounter.increment()
              dbApi.columnFamily(sampleDb.versionstampValueTest).putTask(
                "foo",
                TestValueWithVersionstamp(Versionstamp.incomplete())
              )
            }
            .repeat(Schedule.forever)
            .unit
        )
        .add(
          "interruptor",
          ZIO.unit.delay(5.seconds.asJava)
        )
        .run()
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
