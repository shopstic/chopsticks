package dev.chopsticks.fp

import com.typesafe.config.ConfigRenderOptions
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.config.HoconConfig
import zio.RIO

object ZAkkaAppTest extends ZPekkoApp {
  override def run: RIO[ZAkkaAppEnv, Any] = {
    HoconConfig
      .get
      .flatMap { cfg =>
        zio.Console.printLine(cfg.root().render(ConfigRenderOptions.defaults()))
      }
  }
}
