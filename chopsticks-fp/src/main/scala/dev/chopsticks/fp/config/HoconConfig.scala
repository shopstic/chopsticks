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

  def unsafeResolveConfig(resourceConfigFile: Option[String] = None): Config = {
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

    val appConfig = resourceConfigFile match {
      case Some(configFile) =>
        ConfigFactory
          .parseResources(configFile, ConfigParseOptions.defaults().setAllowMissing(false))

      case None =>
        ConfigFactory.empty()
    }

    val loader = Thread.currentThread.getContextClassLoader
    val parseOptions = ConfigParseOptions.defaults.setAllowMissing(false).setClassLoader(loader)
    val defaultApplication = ConfigFactory.defaultApplication(parseOptions)
    val defaultOverrides = ConfigFactory.defaultOverrides(loader)
    val defaultReference = ConfigImpl.defaultReferenceUnresolved(loader)

    val combinedUnresolvedConfig = defaultOverrides
      .withFallback(entryConfig)
      .withFallback(appConfig)
      .withFallback(defaultApplication)
      .withFallback(defaultReference)

    val combinedResolvedConfig = combinedUnresolvedConfig
      .resolve(ConfigResolveOptions.defaults())

    combinedResolvedConfig
  }

  def liveWithResourceConfigFile(resourceConfigFile: Option[String]): ZLayer[Any, Throwable, HoconConfig] = {
    Task(unsafeResolveConfig(resourceConfigFile))
      .map(cfg =>
        new Service {
          override val config: Config = cfg
        }
      )
      .toLayer
  }

  def live(appClass: Option[Class[_]] = None): ZLayer[Any, Throwable, HoconConfig] = {
    liveWithResourceConfigFile(appClass.map { kclass =>
      val appName = KebabCase.fromTokens(PascalCase.toTokens(kclass.getSimpleName.replace("$", "")))
      kclass.getPackage.getName.replace(".", "/") + "/" + appName + ".conf"
    })
  }
}
