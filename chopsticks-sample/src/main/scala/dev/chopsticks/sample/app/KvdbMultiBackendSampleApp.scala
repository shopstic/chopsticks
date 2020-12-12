package dev.chopsticks.sample.app

import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbDatabaseConfig
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.RocksdbDatabaseConfig
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Definition._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigReader
import zio.{RIO, Task, ZIO, ZLayer}

import java.time.Instant

final case class KvdbMultiBackendSampleAppConfig(fdb: FdbDatabaseConfig, rocksdb: RocksdbDatabaseConfig)

object KvdbMultiBackendSampleAppConfig {
  //noinspection TypeAnnotation
  implicit val configReader = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigReader[KvdbMultiBackendSampleAppConfig]
  }
}

object KvdbMultiBackendSampleApp extends AkkaDiApp[KvdbMultiBackendSampleAppConfig] {
  import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Backends

  override def config(allConfig: Config): Task[KvdbMultiBackendSampleAppConfig] = Task(
    PureconfigLoader.unsafeLoad[KvdbMultiBackendSampleAppConfig](allConfig, "app")
  )

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: KvdbMultiBackendSampleAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val fdbManaged = for {
        backend <- FdbDatabase
          .manage(Backends.fdbStorage, appConfig.fdb)
        api <- KvdbDatabaseApi(backend).toManaged_
      } yield FdbService(api, Backends.fdbStorage)

      val rocksdbManaged = for {
        backend <- RocksdbDatabase
          .manage(Backends.rocksdbStorage, appConfig.rocksdb)
        api <- KvdbDatabaseApi(backend).toManaged_
      } yield RocksdbService(api, Backends.rocksdbStorage)

      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.fromManaged(fdbManaged),
          ZLayer.fromManaged(rocksdbManaged),
          KvdbIoThreadPool.live(),
          ZLayer.succeed(appConfig),
          AppLayer(app)
        )
      )
    }
  }

  private def populate(db: DbService): RIO[AkkaEnv with IzLogging, Int] = {
    Source(1 to 100)
      .flatMapConcat { i =>
        Source(1 to 100)
          .map { j =>
            val key = TestKey(name = s"item $i", Instant.MIN.plusSeconds(j.toLong), version = i * j)
            val value = TestValue(quantity = i.toLong * 123, amount = Math.pow(j.toDouble, 2))
            key -> value
          }
      }
      .toZAkkaSource
      .interruptible
      .via(db.api.columnFamily(db.storage.default).putPairsInBatchesFlow)
      .interruptibleRunWith(Sink.fold(0)((s, b) => s + b.size))
  }

  private def scanAndCollect(
    db: DbService
  ): RIO[AkkaEnv with IzLogging, Seq[(TestKey, TestValue)]] = {
    db
      .api
      .columnFamily(db.storage.default)
      .source(_ startsWith "item 49", _ lt "item 49" -> Instant.MIN.plusSeconds(50))
      .toZAkkaSource
      .interruptibleRunWith(Sink.seq)
  }

  private def lookup(db: DbService, key: TestKey): Task[Option[(TestKey, TestValue)]] = {
    db
      .api
      .columnFamily(db.storage.default)
      .getTask(_ is key)
  }

  //noinspection TypeAnnotation
  def app = {
    for {
      fdbSvc <- ZIO.service[FdbService]
      rocksdbSvc <- ZIO.service[RocksdbService]
      populated <- populate(fdbSvc)
        .logResult("Populate FDB", c => s"populated $c pairs")
        .zipPar(
          populate(rocksdbSvc)
            .logResult("Populate RocksDB", c => s"populated $c pairs")
        )
      _ <- Task {
        assert(populated._1 == populated._2)
      }
      collected <- scanAndCollect(fdbSvc)
        .logResult("Scan and collect from FDB", r => s"collected ${r.size} pairs")
        .zipPar(
          scanAndCollect(rocksdbSvc)
            .logResult("Scan and collect from RocksDB", r => s"collected ${r.size} pairs")
        )
      _ <- Task {
        assert(collected._1 == collected._2)
      }
      _ <- lookup(fdbSvc, TestKey("item 99", Instant.MIN.plusSeconds(30), 2970))
        .logResult("Point lookup from FDB", pair => s"got $pair")
        .zipPar(
          lookup(rocksdbSvc, TestKey("item 99", Instant.MIN.plusSeconds(30), 2970))
            .logResult("Point lookup from RocksDB", pair => s"got $pair")
        )
    } yield ()
  }
}
