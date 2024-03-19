package dev.chopsticks.testkit

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKitBase
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.{KebabCase, PascalCase}

trait AkkaTestKit extends TestKitBase {
  lazy val typesafeConfig: Config = {
    val cfg = ConfigFactory.load()
    assert(
      cfg.getBoolean("pekko.stream.materializer.debug.fuzzing-mode"),
      "pekko.stream.materializer.debug.fuzzing-mode is not 'on' for testing, config loading is not working properly?"
    )
    cfg
  }

  implicit lazy val system: ActorSystem = ActorSystem(
    getClass.getName.split("\\.").map(n => KebabCase.fromTokens(PascalCase.toTokens(n))).mkString("-"),
    typesafeConfig
  )
}
