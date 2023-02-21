//package dev.chopsticks.fp
//
//import com.typesafe.config.ConfigRenderOptions
//import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
//import dev.chopsticks.fp.config.HoconConfig
//import zio.{ExitCode, RIO}
//
//object ZAkkaAppTest extends ZAkkaApp {
//  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
//    HoconConfig
//      .get
//      .flatMap { cfg =>
//        zio.console.putStrLn(cfg.root().render(ConfigRenderOptions.defaults()))
//      }
//      .as(ExitCode(0))
//  }
//}
