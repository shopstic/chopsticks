package dev.chopsticks.util.config

import com.typesafe.config.Config
import japgolly.microlibs.utils.AsciiTable
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.error.{CannotParse, CannotRead, ConfigReaderFailures, ConvertFailure, ThrowableFailure}
import pureconfig.generic.ProductHint

object PureconfigLoader {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  private def sanitizeReason(reason: String) = {
    reason.replace("\n", " ")
  }

  def load[Cfg: ConfigReader](config: Config, namespace: String): Either[String, Cfg] = {
    ConfigSource.fromConfig(config).at(namespace).load[Cfg] match {
      case Left(failures: ConfigReaderFailures) =>
        Left(
          AsciiTable(
            List("Path", "Reason", "Origin") :: {
              failures.toList.map {
                case ConvertFailure(reason, location, path) =>
                  val origin = path match {
                    case "" => ""
                    case _ => location.map(_.toString).getOrElse(config.getValue(path).origin().description())
                  }
                  List(path, sanitizeReason(reason.description), origin)
                case CannotParse(reason, location) =>
                  List("", sanitizeReason(reason), location.toString)
                case ThrowableFailure(e, location) =>
                  List("", sanitizeReason(e.getMessage), location.toString)
                case cannotRead: CannotRead =>
                  List("", sanitizeReason(cannotRead.description), "")
                case _ => ???
              }
            },
            separateDataRows = false
          )
        )
      case Right(cfg) => Right(cfg)
    }
  }

  def unsafeLoad[Cfg: ConfigReader](config: Config, namespace: String): Cfg = {
    load(config, namespace) match {
      case Right(cfg) => cfg
      case Left(error) => throw new IllegalArgumentException("\n" + error)
    }
  }
}
