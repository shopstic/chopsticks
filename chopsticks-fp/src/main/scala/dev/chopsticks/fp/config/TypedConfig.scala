package dev.chopsticks.fp.config

import com.typesafe.config.{ConfigList, ConfigRenderOptions}
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.util.config.PureconfigLoader
import dev.chopsticks.util.config.PureconfigLoader.PureconfigLoadFailure
import izumi.logstage.api.Log
import japgolly.microlibs.utils.AsciiTable
import pureconfig.ConfigReader
import zio.{RLayer, Task, UIO, URIO, ZIO}

import scala.jdk.CollectionConverters._

object TypedConfig {
  trait Service[Cfg] {
    def config: Cfg
  }

  def get[Cfg: zio.Tag]: URIO[TypedConfig[Cfg], Cfg] = ZIO.access[TypedConfig[Cfg]](_.get.config)

  def live[Cfg: ConfigReader: zio.Tag](
    configNamespace: String = "app",
    logLevel: Log.Level = Log.Level.Info
  ): RLayer[IzLogging with HoconConfig, TypedConfig[Cfg]] = {
    val effect = for {
      hoconConfig <- HoconConfig.get
      logger <- IzLogging.logger
      result <- Task.effectSuspend {
        val debugInfo = AsciiTable(
          List("Key", "Value", "Origin") ::
            hoconConfig
              .getConfig(configNamespace)
              .entrySet()
              .asScala
              .toList
              .sortBy(_.getKey)
              .map { entry =>
                val origin = entry.getValue.origin().description().replaceFirst(" @ (.+): (\\d+)", ": $2")
                val value = entry.getValue

                val renderedValue = value match {
                  case list: ConfigList =>
                    list.iterator().asScala.map(_.render(ConfigRenderOptions.concise())).mkString("[", ", ", "]")
                  case v => v.render(ConfigRenderOptions.concise())
                }

                List(configNamespace + "." + entry.getKey, renderedValue, origin)
              }
        )

        logger.log(logLevel)(s"Provided ${configNamespace -> "" -> null} config:\n${debugInfo -> "" -> null}")

        ZIO
          .fromEither(PureconfigLoader.load[Cfg](hoconConfig, configNamespace))
          .mapError { error =>
            PureconfigLoadFailure(
              s"Failed converting HOCON config to ${zio.Tag[Cfg].closestClass.getName}. Reasons:\n" + error
            )
          }
          .tapError(error => UIO(logger.error(s"${error.getMessage -> "" -> null}")))
      }
    } yield {
      new Service[Cfg] {
        override val config: Cfg = result
      }
    }

    effect.toLayer
  }
}
