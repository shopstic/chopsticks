package dev.chopsticks.kvdb

import cats.Show
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions
import org.rocksdb.CompressionType
import pureconfig.ConfigConvert
import pureconfig.ConfigConvert.viaNonEmptyString
import pureconfig.error.FailureReason
import pureconfig.generic.FieldCoproductHint
import squants.information.Information

import scala.concurrent.duration._

object DbFactory {

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

    implicit private val informationShow: Show[Information] = _.toString()
    implicit private val compressionTypeShow: Show[CompressionType] = (t: CompressionType) =>
      Option(t.getLibraryName).getOrElse("none")
    implicit val dbClientConfigShow: Show[DbClientConfig] = {
      import cats.derived.auto.showPretty._
      import cats.implicits._
      cats.derived.semi.showPretty
    }
  }

  final case class RocksdbColumnFamilyConfig(
    memoryBudget: Option[Information],
    blockCache: Option[Information],
    blockSize: Option[Information],
    compression: Option[CompressionType]
  )

  object RocksdbColumnFamilyConfig {
    import dev.chopsticks.util.config.PureconfigConverters._

    final case class InvalidCompressionType(given: String) extends FailureReason {
      def description: String =
        s"Invalid RocksDB compression type '$given', valid types are: " +
          s"${(CompressionType.values.map(_.getLibraryName).collect { case c if c ne null => s"'$c'" } :+ "'none'").mkString(", ")}"
    }

    implicit val compressionTypeConverter: ConfigConvert[CompressionType] = viaNonEmptyString[CompressionType](
      {
        case "none" => Right(CompressionType.NO_COMPRESSION)
        case s =>
          CompressionType.values.find(_.getLibraryName == s).map(Right(_)).getOrElse(Left(InvalidCompressionType(s)))
      },
      _.getLibraryName
    )

    //noinspection TypeAnnotation
    implicit val converter = ConfigConvert[RocksdbColumnFamilyConfig]
  }

  final case class RocksdbDbClientConfig(
    path: String,
    readOnly: Boolean,
    startWithBulkInserts: Boolean,
    checksumOnRead: Boolean,
    syncWriteBatch: Boolean,
    useDirectIo: Boolean,
    columns: Map[String, RocksdbColumnFamilyConfig],
    ioDispatcher: String
  ) extends DbClientConfig

  final case class LmdbDbClientConfig(
    path: String,
    maxSize: Information,
    noSync: Boolean,
    ioDispatcher: String
  ) extends DbClientConfig

  final case class RemoteDbClientConfig(
    host: String,
    port: Int,
    useCompression: Boolean,
    keepAliveInterval: FiniteDuration,
    iterateFailureDelayIncrement: FiniteDuration,
    iterateFailureDelayResetAfter: FiniteDuration,
    iterateFailureMaxDelay: FiniteDuration
  ) extends DbClientConfig

  final case class RocksdbServerConfig(port: Int)

  def client[DbDef <: DbDefinition](
    definition: DbDef,
    config: DbClientConfig
  )(implicit akkaEnv: AkkaEnv): DbClient[DbDef] = {
    DbClient[DbDef](apply[DbDef](definition, config))
  }

  def apply[DbDef <: DbDefinition](
    definition: DbDef,
    config: DbClientConfig
  )(implicit akkaEnv: AkkaEnv): DbInterface[DbDef] = {
    config match {
      case RocksdbDbClientConfig(
          path,
          readOnly,
          startWithBulkInserts,
          checksumOnRead,
          syncWriteBatch,
          useDirectIo,
          columns,
          ioDispatcher
          ) =>
        //noinspection RedundantCollectionConversion
        val customCfOptions: Map[DbDef#BaseCol[_, _], RocksdbCFOptions] = columns.map {
          case (k, customOptions: RocksdbColumnFamilyConfig) =>
            val col = definition.columns.withName(k).asInstanceOf[DbDef#BaseCol[_, _]]
            val defaultRocksdbOptions = col.rocksdbOptions
            (
              col,
              RocksdbCFOptions(
                memoryBudget = customOptions.memoryBudget.getOrElse(defaultRocksdbOptions.memoryBudget),
                blockCache = customOptions.blockCache.getOrElse(defaultRocksdbOptions.blockCache),
                blockSize = customOptions.blockSize.getOrElse(defaultRocksdbOptions.blockSize),
                readPattern = defaultRocksdbOptions.readPattern,
                compression = customOptions.compression.getOrElse(defaultRocksdbOptions.compression)
              )
            )
        }.toMap

        RocksdbDb[DbDef](
          definition = definition,
          path = path,
          options = customCfOptions,
          readOnly = readOnly,
          startWithBulkInserts = startWithBulkInserts,
          checksumOnRead = checksumOnRead,
          syncWriteBatch = syncWriteBatch,
          useDirectIo = useDirectIo,
          ioDispatcher = ioDispatcher
        )

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
