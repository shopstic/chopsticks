package dev.chopsticks.fp.iz_logging

import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import izumi.logstage.api.Log.Level
import pureconfig.ConfigReader
import pureconfig.generic.FieldCoproductHint

sealed trait IzLoggingFormat
final case class IzLoggingTextFormat(withExceptions: Boolean, withoutColors: Boolean) extends IzLoggingFormat
final case class IzLoggingJsonFormat(pretty: Boolean) extends IzLoggingFormat
object IzLoggingFormat {
  implicit val hint: FieldCoproductHint[IzLoggingFormat] =
    new FieldCoproductHint[IzLoggingFormat]("type") {
      override def fieldValue(name: String): String =
        name.drop("IzLogging".length).dropRight("Format".length).toLowerCase()
    }
}

sealed trait IzLoggingDestination
final case class IzLoggingConsoleDestination() extends IzLoggingDestination
final case class IzLoggingFileDestination(
  path: NonEmptyString,
  rotationMaxFileCount: PosInt,
  rotationMaxFileBytes: PosInt
) extends IzLoggingDestination
final case class IzLoggingTcpDestination(host: NonEmptyString, port: PortNumber, bufferSize: PosInt)
    extends IzLoggingDestination
object IzLoggingDestination {
  implicit val hint: FieldCoproductHint[IzLoggingDestination] =
    new FieldCoproductHint[IzLoggingDestination]("type") {
      override def fieldValue(name: String): String =
        name.drop("IzLogging".length).dropRight("Destination".length).toLowerCase()
    }
}

final case class IzLoggingSinkConfig(enabled: Boolean, format: IzLoggingFormat, destination: IzLoggingDestination)
final case class IzLoggingConfig(level: Level, sinks: Map[String, IzLoggingSinkConfig])

object IzLoggingConfig {
  import dev.chopsticks.util.config.PureconfigConverters._
  implicit val levelConfigReader: ConfigReader[Level] =
    ConfigReader.fromString(l => Right(Level.parseSafe(l, Level.Info)))
  //noinspection TypeAnnotation
  implicit val configReader = ConfigReader[IzLoggingConfig]
}
