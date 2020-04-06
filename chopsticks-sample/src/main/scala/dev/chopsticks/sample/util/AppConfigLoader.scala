package dev.chopsticks.sample.util

import com.typesafe.config.{Config, ConfigList, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.util.config.PureconfigLoader
import japgolly.microlibs.utils.AsciiTable
import pureconfig.ConfigReader

import scala.jdk.CollectionConverters._

object AppConfigLoader extends StrictLogging {
  def load[Cfg: ConfigReader](config: Config, configNamespace: String = "app"): Cfg = {
    val debugInfo = AsciiTable(
      List("Key", "Value", "Origin") ::
        config
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

    logger.info(s"Provided $configNamespace config:\n${debugInfo}")
    PureconfigLoader.unsafeLoad[Cfg](config, configNamespace)
  }
}
