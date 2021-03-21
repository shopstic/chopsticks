package dev.chopsticks.fp.config

import com.typesafe.config.impl.ConfigImpl
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.{KebabCase, PascalCase}
import zio.{Task, URIO, ZIO, ZLayer}

import java.nio.file.Paths

object HoconConfig {
  trait Service {
    def config: Config
  }

  def get: URIO[HoconConfig, Config] = ZIO.access[HoconConfig](_.get.config)

  def live(appClass: Option[Class[_]] = None): ZLayer[Any, Throwable, HoconConfig] = {
    Task {
      if (scala.sys.props.get("config.file").nonEmpty) {
        System.err.println(
          "System property 'config.file' was set, but should no longer be used " +
            "since it conflicts with Lightbend Config loader. Use 'config.entry' instead"
        )
      }

      val entryConfig = scala.sys.props.get("config.entry") match {
        case Some(customConfigFile) =>
          ConfigFactory
            .parseFile(Paths.get(customConfigFile).toFile, ConfigParseOptions.defaults().setAllowMissing(false))
        case None =>
          ConfigFactory.empty()
      }

      val appConfig = appClass match {
        case Some(kclass) =>
          val appName = KebabCase.fromTokens(PascalCase.toTokens(kclass.getSimpleName.replace("$", "")))
          val appConfigName = kclass.getPackage.getName.replace(".", "/") + "/" + appName + ".conf"

          ConfigFactory
            .parseResources(appConfigName, ConfigParseOptions.defaults().setAllowMissing(false))

        case None =>
          ConfigFactory.empty()
      }

      val defaultConfig =
        ConfigFactory.load(ConfigParseOptions.defaults.setAllowMissing(false), ConfigResolveOptions.defaults)

      val loader = Thread.currentThread.getContextClassLoader
      val parseOptions = ConfigParseOptions.defaults.setAllowMissing(false).setClassLoader(loader)
      val defaultApplication = ConfigFactory.defaultApplication(parseOptions)
      val defaultOverrides = ConfigFactory.defaultOverrides(loader)
      val defaultReference = ConfigImpl.defaultReferenceUnresolved(loader)

      val combinedUnresolvedConfig = entryConfig
        .withFallback(appConfig)
        .withFallback(defaultOverrides)
        .withFallback(defaultApplication)
        .withFallback(defaultReference)

      val combinedResolvedConfig = combinedUnresolvedConfig
        .resolve(ConfigResolveOptions.defaults())

      combinedResolvedConfig
    }
      .map(cfg =>
        new Service {
          override val config: Config = cfg
        }
      )
      .toLayer
  }
}
