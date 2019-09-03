package dev.chopsticks.kvdb.rocksdb

import eu.timepit.refined.types.numeric.PosInt
import org.rocksdb.CompressionType
import pureconfig.ConfigConvert
import pureconfig.error.FailureReason
import squants.information.Information

object RocksdbColumnFamilyConfig {
  sealed trait ReadPattern

  object PointLookupPattern extends ReadPattern
  object TotalOrderScanPattern extends ReadPattern
  final case class PrefixedScanPattern(minPrefixLength: PosInt) extends ReadPattern

  final case class InvalidCompressionType(given: String) extends FailureReason {
    def description: String =
      s"Invalid RocksDB compression type '$given', valid types are: " +
        s"${(CompressionType.values.map(_.getLibraryName).collect { case c if c ne null => s"'$c'" } :+ "'none'").mkString(", ")}"
  }

  implicit val compressionTypeConvert: ConfigConvert[CompressionType] =
    ConfigConvert.viaNonEmptyString[CompressionType](
      {
        case "none" => Right(CompressionType.NO_COMPRESSION)
        case s =>
          CompressionType.values.find(_.getLibraryName == s).map(Right(_)).getOrElse(Left(InvalidCompressionType(s)))
      },
      _.getLibraryName
    )
}

final case class RocksdbColumnFamilyConfig(
  memoryBudget: Information,
  blockCache: Information,
  blockSize: Information,
  readPattern: RocksdbColumnFamilyConfig.ReadPattern,
  compression: CompressionType = CompressionType.NO_COMPRESSION
)
