package dev.chopsticks.sample.app

import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.RocksdbDatabaseConfig
import dev.chopsticks.kvdb.rocksdb.{RocksdbColumnFamilyOptionsMap, RocksdbDatabase, RocksdbMaterialization}
import dev.chopsticks.kvdb.util.KvdbException.SeekFailure
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool}
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbMaterialization}
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb
import dev.chopsticks.sample.kvdb.MultiBackendSampleDb.Definition.{BaseCf, CfSet}
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.types.string.NonEmptyString
import pureconfig.ConfigConvert
import zio.{RIO, ZIO, ZLayer}

final case class DynamicSubsetKvdbSampleAppConfig(subsetIds: Set[NonEmptyString], rocksdb: RocksdbDatabaseConfig)

object DynamicSubsetKvdbSampleAppConfig {
  import dev.chopsticks.util.config.PureconfigConverters._
  //noinspection TypeAnnotation
  implicit val configConvert = ConfigConvert[DynamicSubsetKvdbSampleAppConfig]
}

abstract class DynamicKvdbStorage extends KvdbMaterialization[BaseCf, CfSet] with RocksdbMaterialization[BaseCf, CfSet]

final case class DynamicKvdbService(api: KvdbDatabaseApi[BaseCf], storage: DynamicKvdbStorage)

object DynamicSubsetKvdbSampleApp extends ZPekkoApp {
  //noinspection TypeAnnotation
  def app = {
    for {
      dbService <- ZIO.service[DynamicKvdbService]
      _ <- ZIO.foreachDiscard(dbService.storage.columnFamilySet.value) { cf =>
        dbService
          .api
          .columnFamily(cf)
          .source
          .toZAkkaSource
          .killSwitch
          .interruptibleRunIgnore()
          .catchSome {
            case _: SeekFailure => ZIO.unit
          }
          .log(s"cf=${cf.id}")
      }
    } yield ()
  }

  override def run: RIO[ZAkkaAppEnv, Any] = {
    import MultiBackendSampleDb.Backends.rocksdbStorage

    val rocksdbManaged = for {
      appConfig <- TypedConfig.get[DynamicSubsetKvdbSampleAppConfig]
      subsetIds = appConfig.subsetIds.map(_.value)
      storage = new DynamicKvdbStorage {
        override val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] =
          rocksdbStorage.columnFamilySet.filter(cf => subsetIds.contains(cf.id))
        override val columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] =
          rocksdbStorage.columnFamilyConfigMap.filter { case (cf, _) => subsetIds.contains(cf.id) }
        override val defaultColumnFamily: MultiBackendSampleDb.Definition.BaseCf[_, _] = rocksdbStorage.default
      }
      backend <- RocksdbDatabase.manage(storage, appConfig.rocksdb)
      api <- KvdbDatabaseApi(backend)
    } yield DynamicKvdbService(api, storage)

    app
      .provideSome[ZAkkaAppEnv](
        TypedConfig.live[DynamicSubsetKvdbSampleAppConfig](),
        ZLayer.scoped(rocksdbManaged),
        KvdbIoThreadPool.live,
        KvdbSerdesThreadPool.fromDefaultPekkoDispatcher()
      )
  }
}
