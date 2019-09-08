package dev.chopsticks.kvdb.rocksdb

import eu.timepit.refined.types.numeric.PosInt
import org.rocksdb.{ColumnFamilyOptions, CompressionType}
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

  //noinspection TypeAnnotation
  implicit val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[RocksdbColumnFamilyConfig]
  }
}

final case class RocksdbColumnFamilyConfig(
  memoryBudget: Information,
  blockCache: Information,
  blockSize: Information,
  compression: CompressionType = CompressionType.NO_COMPRESSION
) {
  import RocksdbColumnFamilyConfig._

  def toOptions(readPattern: ReadPattern): ColumnFamilyOptions = {
    val cfBuilder = RocksdbColumnFamilyOptionsBuilder(this)
    val tunedCfBuilder = readPattern match {
      case PointLookupPattern =>
        cfBuilder.withPointLookup()
      case PrefixedScanPattern(minPrefixLength) =>
        cfBuilder.withCappedPrefixExtractor(minPrefixLength.value)
      case TotalOrderScanPattern =>
        cfBuilder.withTotalOrder()
    }

    tunedCfBuilder.build()
  }
}
