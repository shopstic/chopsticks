package dev.chopsticks.util.config

import com.typesafe.config.Config
import japgolly.microlibs.utils.AsciiTable
import pureconfig.ConfigReader
import pureconfig.error.{CannotParse, ConfigReaderFailures, ConvertFailure, ThrowableFailure}

object PureconfigLoader {
  def load[Cfg: ConfigReader](config: Config, namespace: String): Either[String, Cfg] = {
    pureconfig.loadConfig[Cfg](config, namespace) match {
      case Left(failures: ConfigReaderFailures) =>
        Left(
          AsciiTable(
            List("Path", "Reason", "Origin") :: failures.toList.map {
              case ConvertFailure(reason, location, path) =>
                val origin = path match {
                  case "" => ""
                  case _ => location.map(_.toString).getOrElse(config.getValue(path).origin().description())
                }
                List(path, reason.description, origin)
              case CannotParse(reason, location) =>
                List("", reason, location.toString)
              case ThrowableFailure(e, location) =>
                List("", e.getMessage, location.toString)
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
