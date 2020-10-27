package dev.chopsticks.sample.app

import java.time.Instant

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbDatabaseConfig
import dev.chopsticks.kvdb.fdb.FdbMaterialization.{KeyspaceWithVersionstampKey, KeyspaceWithVersionstampValue}
import dev.chopsticks.kvdb.fdb.{FdbDatabase, FdbMaterialization}
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.PrefixedScanPattern
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.RocksdbDatabaseConfig
import dev.chopsticks.kvdb.rocksdb.{
  RocksdbColumnFamilyConfig,
  RocksdbColumnFamilyOptionsMap,
  RocksdbDatabase,
  RocksdbMaterialization
}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import dev.chopsticks.sample.app.SampleDbBackends.{DbService, FdbService, RocksdbService}
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import eu.timepit.refined.auto._
import pureconfig.ConfigReader
import zio.{RIO, Task, ZIO, ZLayer}
import dev.chopsticks.fp.zio_ext._

final case class KvdbMultiBackendSampleAppConfig(fdb: FdbDatabaseConfig, rocksdb: RocksdbDatabaseConfig)

object KvdbMultiBackendSampleAppConfig {
  //noinspection TypeAnnotation
  implicit val configReader = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigReader[KvdbMultiBackendSampleAppConfig]
  }
}

object SampleDbDefinition extends KvdbDefinition {
  final case class TestKey(name: String, time: Instant, version: Int)
  final case class TestValue(quantity: Long, amount: Double)

  trait Default extends BaseCf[TestKey, TestValue]

  type CfSet = Default

  trait DbStorage extends KvdbMaterialization[BaseCf, CfSet] {
    def default: Default
    override def columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = ColumnFamilySet[BaseCf].of(default)
  }

  trait FdbStorage extends DbStorage with FdbMaterialization[BaseCf] {
    override def keyspacesWithVersionstampKey: Set[KeyspaceWithVersionstampKey[BaseCf]] = Set.empty
    override def keyspacesWithVersionstampValue: Set[KeyspaceWithVersionstampValue[BaseCf]] = Set.empty
  }

  trait RocksdbStorage extends DbStorage with RocksdbMaterialization[BaseCf, CfSet] {
    override def defaultColumnFamily: BaseCf[_, _] = default
    override def columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] = {
      import squants.information.InformationConversions._

      RocksdbColumnFamilyOptionsMap[BaseCf]
        .of(
          default,
          RocksdbColumnFamilyConfig(
            memoryBudget = 1.mib,
            blockCache = 1.mib,
            blockSize = 8.kib,
            writeBufferCount = 4
          ).toOptions(PrefixedScanPattern(1))
        )
    }
  }
}

object SampleDbBackends {
  import SampleDbDefinition._

  sealed trait DbService {
    def api: KvdbDatabaseApi[BaseCf]
    def storage: DbStorage
  }

  final case class FdbService(api: KvdbDatabaseApi[BaseCf], storage: FdbStorage) extends DbService
  final case class RocksdbService(api: KvdbDatabaseApi[BaseCf], storage: RocksdbStorage) extends DbService

  object fdbStorage extends FdbStorage {
    import dev.chopsticks.kvdb.codec.fdb_key._
    implicit val valueSerdes: ValueSerdes[TestValue] = ValueSerdes.fromKeySerdes

    object default extends Default
  }

  object rocksdbStorage extends RocksdbStorage {
    import dev.chopsticks.kvdb.codec.berkeleydb_key._
    implicit val valueSerdes: ValueSerdes[TestValue] = ValueSerdes.fromKeySerdes

    object default extends Default
  }
}

object KvdbMultiBackendSampleApp extends AkkaDiApp[KvdbMultiBackendSampleAppConfig] {
  import SampleDbDefinition._

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
          .manage(SampleDbBackends.fdbStorage, appConfig.fdb)
        api <- KvdbDatabaseApi(backend).toManaged_
      } yield FdbService(api, SampleDbBackends.fdbStorage)

      val rocksdbManaged = for {
        backend <- RocksdbDatabase
          .manage(SampleDbBackends.rocksdbStorage, appConfig.rocksdb)
        api <- KvdbDatabaseApi(backend).toManaged_
      } yield RocksdbService(api, SampleDbBackends.rocksdbStorage)

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

  private def populate(db: DbService): RIO[AkkaEnv with LogEnv, Int] = {
    ZAkkaStreams
      .interruptibleGraph(
        Source(1 to 100)
          .flatMapConcat { i =>
            Source(1 to 100)
              .map { j =>
                val key = TestKey(name = s"item $i", Instant.MIN.plusSeconds(j.toLong), version = i * j)
                val value = TestValue(quantity = i.toLong * 123, amount = Math.pow(j.toDouble, 2))
                key -> value
              }
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .via(db.api.columnFamily(db.storage.default).putPairsInBatchesFlow)
          .toMat(Sink.fold(0)((s, b) => s + b.size))(Keep.both),
        graceful = true
      )
  }

  private def scanAndCollect(
    db: DbService
  ): RIO[AkkaEnv with LogEnv, Seq[(TestKey, TestValue)]] = {
    ZAkkaStreams
      .interruptibleGraph(
        db
          .api
          .columnFamily(db.storage.default)
          .source(_ startsWith "item 49", _ lt "item 49" -> Instant.MIN.plusSeconds(50))
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.seq)(Keep.both),
        graceful = true
      )
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
    } yield ()
  }
}
