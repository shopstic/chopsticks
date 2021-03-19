package dev.chopsticks.fp.config

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.{KebabCase, PascalCase}
import zio.{Has, Task, URIO, ZIO, ZLayer}

import java.nio.file.Paths

object HoconConfig {
  trait Service {
    def config: Config
  }

  def get: URIO[HoconConfig, Config] = ZIO.access[HoconConfig](_.get.config)

  def load: ZLayer[Any, Throwable, HoconConfig] = {
    (for {
      loaded <- Task(ConfigFactory.load())
    } yield {
      new Service {
        override val config: Config = loaded
      }
    }).toLayer
  }

  def live(appClass: Class[_]): ZLayer[Any, Throwable, HoconConfig] = {
    Task {
      val appName = KebabCase.fromTokens(PascalCase.toTokens(appClass.getSimpleName.replace("$", "")))
      val appConfigName = appClass.getPackage.getName.replace(".", "/") + "/" + appName

      val customAppConfig = scala.sys.props.get("config.file") match {
        case Some(customConfigFile) =>
          ConfigFactory
            .parseFile(Paths.get(customConfigFile).toFile, ConfigParseOptions.defaults().setAllowMissing(false))
            .resolve(ConfigResolveOptions.defaults())
        case None =>
          ConfigFactory.empty()
      }

      val config = customAppConfig.withFallback(
        ConfigFactory.load(
          appConfigName,
          ConfigParseOptions.defaults.setAllowMissing(false),
          ConfigResolveOptions.defaults
        )
      )

      config
    }
      .map(cfg =>
        new Service {
          override val config: Config = cfg
        }
      )
      .toLayer
  }
}
