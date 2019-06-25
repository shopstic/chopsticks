package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions
import pureconfig.generic.FieldCoproductHint
import pureconfig.{KebabCase, PascalCase}
import squants.information.Information

import scala.concurrent.duration._

object DbFactory {
  private val DEFAULT_DB_IO_DISPATCHER = "dev.chopsticks.kvdb.db-io-dispatcher"

  sealed trait DbClientConfig

  object DbClientConfig {
    //noinspection TypeAnnotation
    implicit val hint = new FieldCoproductHint[DbClientConfig]("type") {
      override def fieldValue(name: String): String = name.dropRight("DbClientConfig".length).toLowerCase()
    }

    def engineName(config: DbClientConfig): String = {
      config match {
        case _: RocksdbDbClientConfig => "rocksdb"
        case _: LmdbDbClientConfig => "lmdb"
        case _: RemoteDbClientConfig => "remote"
      }
    }
  }

  final case class RocksdbColumnFamilyConfig(memoryBudget: Information, blockCache: Information)

  final case class RocksdbDbClientConfig(
    path: String,
    readOnly: Boolean = false,
    startWithBulkInserts: Boolean = false,
    columns: Map[String, RocksdbColumnFamilyConfig] = Map.empty[String, RocksdbColumnFamilyConfig],
    ioDispatcher: String = DEFAULT_DB_IO_DISPATCHER
  ) extends DbClientConfig

  final case class LmdbDbClientConfig(
    path: String,
    maxSize: Information,
    noSync: Boolean = false,
    ioDispatcher: String = DEFAULT_DB_IO_DISPATCHER
  ) extends DbClientConfig

  final case class RemoteDbClientConfig(
    host: String,
    port: Int,
    useCompression: Boolean = false,
    keepAliveInterval: FiniteDuration = 30.seconds,
    iterateFailureDelayIncrement: FiniteDuration = 10.millis,
    iterateFailureDelayResetAfter: FiniteDuration = 500.millis,
    iterateFailureMaxDelay: FiniteDuration = 1.second
  ) extends DbClientConfig

  final case class RocksdbServerConfig(port: Int)

  def client[DbDef <: DbDefinition](
    definition: DbDef,
    config: DbClientConfig
  )(implicit akkaEnv: AkkaEnv): DbClient[DbDef] = {
    DbClient[DbDef](apply[DbDef](definition, config))
  }

  def remoteClient[DbDef <: DbDefinition](
    definition: DbDef,
    name: Option[String] = None
  )(implicit akkaEnv: AkkaEnv): DbClient[DbDef] = {
    val srvName = name.getOrElse(
      KebabCase.fromTokens(PascalCase.toTokens(definition.getClass.getSimpleName.replaceAllLiterally("$", "")))
    )
    val host = s"$srvName.marathon.l4lb.thisdcos.directory"
    val config = RemoteDbClientConfig(host = host, port = 80)
    client(definition, config)
  }

  def apply[DbDef <: DbDefinition](
    definition: DbDef,
    config: DbClientConfig
  )(implicit akkaEnv: AkkaEnv): DbInterface[DbDef] = {
    config match {
      case RocksdbDbClientConfig(path, readOnly, startWithBulkInserts, columns, ioDispatcher) =>
        //noinspection RedundantCollectionConversion
        val customCfOptions: Map[DbDef#BaseCol[_, _], RocksdbCFOptions] = columns.map {
          case (k, v) =>
            (
              definition.columns.withName(k).asInstanceOf[DbDef#BaseCol[_, _]],
              RocksdbCFOptions(
                memoryBudget = v.memoryBudget,
                blockCache = v.blockCache,
                minPrefixLength = 0
              )
            )
        }.toMap

        RocksdbDb[DbDef](definition, path, customCfOptions, readOnly, startWithBulkInserts, ioDispatcher)

      case LmdbDbClientConfig(path, maxSize, noSync, ioDispatcher) =>
        LmdbDb[DbDef](definition, path, maxSize.toBytes.toLong, noSync, ioDispatcher)

      case RemoteDbClientConfig(
          host,
          port,
          _,
          keepAliveInterval,
          iterateFailureDelayIncrement,
          iterateFailureDelayResetAfter,
          iterateFailureMaxDelay
          ) =>
        HttpDb[DbDef](
          definition,
          host,
          port,
          keepAliveInterval,
          iterateFailureDelayIncrement,
          iterateFailureDelayResetAfter,
          iterateFailureMaxDelay
        )
    }
  }
}
