package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.RocksdbDatabaseConfig
import dev.chopsticks.kvdb.rocksdb.{RocksdbColumnFamilyOptionsMap, RocksdbDatabase, RocksdbMaterialization}
import dev.chopsticks.kvdb.util.KvdbException.SeekFailure
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbMaterialization}
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Definition.{BaseCf, CfSet}
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import dev.chopsticks.util.config.PureconfigLoader
import eu.timepit.refined.types.string.NonEmptyString
import pureconfig.ConfigConvert
import zio.{Task, ZIO, ZLayer}

final case class DynamicSubsetKvdbSampleAppConfig(subsetIds: Set[NonEmptyString], rocksdb: RocksdbDatabaseConfig)

object DynamicSubsetKvdbSampleAppConfig {
  import dev.chopsticks.util.config.PureconfigConverters._
  //noinspection TypeAnnotation
  implicit val configConvert = ConfigConvert[DynamicSubsetKvdbSampleAppConfig]
}

abstract class DynamicKvdbStorage extends KvdbMaterialization[BaseCf, CfSet] with RocksdbMaterialization[BaseCf, CfSet]

final case class DynamicKvdbService(api: KvdbDatabaseApi[BaseCf], storage: DynamicKvdbStorage)

object DynamicSubsetKvdbSampleApp extends AkkaDiApp[DynamicSubsetKvdbSampleAppConfig] {
  override def config(allConfig: Config): Task[DynamicSubsetKvdbSampleAppConfig] = Task(
    PureconfigLoader.unsafeLoad[DynamicSubsetKvdbSampleAppConfig](allConfig, "app")
  )

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: DynamicSubsetKvdbSampleAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      import MultiBackendSampleDb.Backends.rocksdbStorage

      val subsetIds = appConfig.subsetIds.map(_.value)
      val storage = new DynamicKvdbStorage {
        override val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] =
          rocksdbStorage.columnFamilySet.filter(cf => subsetIds.contains(cf.id))
        override val columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] =
          rocksdbStorage.columnFamilyConfigMap.filter { case (cf, _) => subsetIds.contains(cf.id) }
        override val defaultColumnFamily: MultiBackendSampleDb.Definition.BaseCf[_, _] = rocksdbStorage.default
      }

      val rocksdbManaged = for {
        backend <- RocksdbDatabase.manage(storage, appConfig.rocksdb)
        api <- KvdbDatabaseApi(backend).toManaged_
      } yield DynamicKvdbService(api, storage)

      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.fromManaged(rocksdbManaged),
          KvdbIoThreadPool.live(),
          ZLayer.succeed(appConfig),
          AppLayer(app)
        )
      )
    }
  }

  //noinspection TypeAnnotation
  def app = {
    for {
      dbService <- ZIO.service[DynamicKvdbService]
      _ <- ZIO.foreach_(dbService.storage.columnFamilySet.value) { cf =>
        dbService
          .api
          .columnFamily(cf)
          .source
          .toZAkkaSource
          .interruptibleRunIgnore()
          .catchSome {
            case _: SeekFailure => ZIO.unit
          }
          .log(s"cf=${cf.id}")
      }
    } yield ()
  }
}
