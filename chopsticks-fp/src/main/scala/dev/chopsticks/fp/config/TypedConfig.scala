package dev.chopsticks.fp.config

import japgolly.microlibs.utils.AsciiTable
import pureconfig.ConfigReader
import dev.chopsticks.util.config.PureconfigLoader
import dev.chopsticks.util.config.PureconfigLoader.PureconfigLoadFailure
import com.typesafe.config.{ConfigList, ConfigRenderOptions}
import zio.{LogLevel, RLayer, URIO, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

trait TypedConfig[Cfg]:
  def config: Cfg

object TypedConfig:
  def get[Cfg: zio.Tag]: URIO[TypedConfig[Cfg], Cfg] =
    ZIO.service[TypedConfig[Cfg]].map(_.config)

  def live[Cfg: ConfigReader: zio.Tag](
    configNamespace: String = "app",
    logLevel: LogLevel = LogLevel.Info,
    maskedKeys: Set[Regex] = Set.empty
  ): RLayer[HoconConfig, TypedConfig[Cfg]] =
    val effect =
      for
        hoconConfig <- HoconConfig.config
        result <- ZIO.suspend {
          ZIO.attempt {}
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
                  val key = entry.getKey

                  val renderedValue =
                    if (maskedKeys.exists(_.matches(key))) {
                      "<redacted secret>"
                    }
                    else {
                      value match {
                        case list: ConfigList =>
                          list.iterator().asScala.map(_.render(ConfigRenderOptions.concise())).mkString("[", ", ", "]")
                        case v => v.render(ConfigRenderOptions.concise())
                      }
                    }

                  List(s"$configNamespace.$key", renderedValue, origin)
                }
          )

          ZIO.logLevel(logLevel) {
            ZIO.log(s"Provided $configNamespace config:\n$debugInfo")
          }

          ZIO
            .fromEither(PureconfigLoader.load[Cfg](hoconConfig, configNamespace))
            .mapError { error =>
              PureconfigLoadFailure(
                s"Failed converting HOCON config to ${zio.Tag[Cfg].closestClass.getName}. Reasons:\n" + error
              )
            }
        }
      yield new TypedConfig[Cfg]:
        override val config: Cfg = result

    ZLayer.fromZIO(effect)
  end live
end TypedConfig
