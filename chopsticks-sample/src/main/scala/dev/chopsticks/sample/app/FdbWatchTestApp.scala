package dev.chopsticks.sample.app

import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.{FullAppEnv, ZAkkaAppEnv}
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool}
import dev.chopsticks.sample.kvdb.SampleDb.TestValueWithVersionstamp
import dev.chopsticks.sample.kvdb.SampleDb
import dev.chopsticks.stream.ZAkkaGraph.InterruptibleGraphOps
import pureconfig.ConfigReader
import zio._

import java.util.concurrent.atomic.{AtomicReference, LongAdder}
import scala.annotation.nowarn

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

object FdbWatchTestApp extends ZPekkoApp {

  object sampleDb extends SampleDb.Materialization {
    import dev.chopsticks.kvdb.codec.fdb_key._
    import dev.chopsticks.kvdb.codec.protobuf_value._
    import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue
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

  @nowarn("cat=lint-infer-any")
  def app = {
    for {
      db <- ZIO.service[SampleDb.Db]
      watchCounter = new LongAdder()
      changeCounter = new LongAdder()
      lastAtomic = new AtomicReference(Option.empty[TestValueWithVersionstamp])
      dbApi <- KvdbDatabaseApi(db)
      zLogger <- IzLogging.zioLogger
      actorSystem <- PekkoEnv.actorSystem
      _ <- LoggedRace()
        .add(
          "logging",
          ZIO
            .suspend {
              val watchCount = watchCounter.longValue()
              val changeCount = changeCounter.longValue()

              zLogger.info(
                s"Status update $watchCount $changeCount ${lastAtomic.get -> "lastVersionstamp"}"
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
            .onInterrupt(ZIO.succeed(println("Intentionally hang here for 5 seconds")) *> ZIO.unit.delay(
              java.time.Duration.ofSeconds(5)
            ))
        )
        .add(
          "update",
          ZIO
            .suspend {
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
          ZIO.unit.delay(1.hour.asJava)
        )
        .run()
    } yield ()
  }

  override def run: RIO[FullAppEnv, Any] = {
    val dbLayer = ZLayer.scoped(
      for {
        appConfig <- TypedConfig.get[FdbTestSampleAppConfig]
        db <- FdbDatabase.manage(sampleDb, appConfig.db)
      } yield db
    )

    app
      .provideSome[ZAkkaAppEnv](
        TypedConfig.live[FdbTestSampleAppConfig](),
        KvdbIoThreadPool.live,
        KvdbSerdesThreadPool.fromDefaultPekkoDispatcher(),
        dbLayer
      )
  }
}
