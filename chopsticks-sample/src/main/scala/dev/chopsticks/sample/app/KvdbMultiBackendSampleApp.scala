package dev.chopsticks.sample.app

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.fdb.FdbDatabase
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbDatabaseConfig
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.RocksdbDatabaseConfig
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool}
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Definition._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import pureconfig.ConfigReader
import zio.{RIO, Task, URIO, ZIO, ZLayer}

import java.time.Instant

final case class KvdbMultiBackendSampleAppConfig(fdb: FdbDatabaseConfig, rocksdb: RocksdbDatabaseConfig)

object KvdbMultiBackendSampleAppConfig {
  //noinspection TypeAnnotation
  implicit val configReader = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigReader[KvdbMultiBackendSampleAppConfig]
  }
}

final class TestKvdbApi[DBS <: DbService] private (db: DBS) {
  def populate: RIO[PekkoEnv with IzLogging, Int] = {
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
      .killSwitch
      .via(db.api.columnFamily(db.storage.default).putPairsInBatchesFlow)
      .interruptibleRunWith(Sink.fold(0)((s, b) => s + b.size))
  }

  def scanAndCollect: RIO[PekkoEnv with IzLogging, Seq[(TestKey, TestValue)]] = {
    db
      .api
      .columnFamily(db.storage.default)
      .source(_ startsWith "item 49", _ lt "item 49" -> Instant.MIN.plusSeconds(50))
      .toZAkkaSource
      .killSwitch
      .interruptibleRunWith(Sink.seq)
  }

  def lookup(key: TestKey): Task[Option[(TestKey, TestValue)]] = {
    db
      .api
      .columnFamily(db.storage.default)
      .getTask(_ is key)
  }
}

object TestKvdbApi {
  def apply[DBS <: DbService: zio.Tag]: URIO[DBS, TestKvdbApi[DBS]] = {
    ZIO.service[DBS].map(db => new TestKvdbApi(db))
  }
}

object KvdbMultiBackendSampleApp extends ZPekkoApp {
  import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Backends

  override def run: RIO[ZAkkaAppEnv, Any] = {
    val fdbManaged = for {
      appConfig <- TypedConfig.get[KvdbMultiBackendSampleAppConfig]
      backend <- FdbDatabase
        .manage(Backends.fdbStorage, appConfig.fdb)
      api <- KvdbDatabaseApi(backend)
    } yield FdbService(api, Backends.fdbStorage)

    val rocksdbManaged = for {
      appConfig <- TypedConfig.get[KvdbMultiBackendSampleAppConfig]
      backend <- RocksdbDatabase
        .manage(Backends.rocksdbStorage, appConfig.rocksdb)
      api <- KvdbDatabaseApi(backend)
    } yield RocksdbService(api, Backends.rocksdbStorage)

    app
      .provideSome[ZAkkaAppEnv](
        TypedConfig.live[KvdbMultiBackendSampleAppConfig](),
        KvdbIoThreadPool.live,
        KvdbSerdesThreadPool.fromDefaultPekkoDispatcher(),
        ZLayer.scoped(fdbManaged),
        ZLayer.scoped(rocksdbManaged)
      )
  }

  //noinspection TypeAnnotation
  def app = {
    for {
      fdbApi <- TestKvdbApi[FdbService]
      rocksdbApi <- TestKvdbApi[RocksdbService]
      populated <- fdbApi
        .populate
        .logResult("Populate FDB", c => s"populated $c pairs")
        .zipPar(
          rocksdbApi
            .populate
            .logResult("Populate RocksDB", c => s"populated $c pairs")
        )
      _ <- ZIO.attempt {
        assert(populated._1 == populated._2)
      }
      collected <- fdbApi
        .scanAndCollect
        .logResult("Scan and collect from FDB", r => s"collected ${r.size} pairs")
        .zipPar(
          rocksdbApi
            .scanAndCollect
            .logResult("Scan and collect from RocksDB", r => s"collected ${r.size} pairs")
        )
      _ <- ZIO.attempt {
        assert(collected._1 == collected._2)
      }
      _ <- fdbApi.lookup(TestKey("item 99", Instant.MIN.plusSeconds(30), 2970))
        .logResult("Point lookup from FDB", pair => s"got $pair")
        .zipPar(
          rocksdbApi
            .lookup(TestKey("item 99", Instant.MIN.plusSeconds(30), 2970))
            .logResult("Point lookup from RocksDB", pair => s"got $pair")
        )
    } yield ()
  }
}
