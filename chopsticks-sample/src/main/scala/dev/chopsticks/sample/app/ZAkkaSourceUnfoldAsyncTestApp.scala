package dev.chopsticks.sample.app

import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Sink
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource
import zio.{durationInt, ExitCode, RIO, ZIO}

object ZAkkaSourceUnfoldAsyncTestApp extends ZPekkoApp {
  override def run: RIO[ZAkkaAppEnv, ExitCode] = {
    ZAkkaSource
      .unfoldAsync(1) { v =>
        ZIO.some((v * 2, v)).delay(1.second).onInterrupt(_ =>
          ZIO.succeed(println(s"Got interrupted v=$v going to delay")) *>
            ZIO.succeed(println("Now release")).delay(2.seconds)
        )
      }
      .viaBuilder(_.buffer(100, OverflowStrategy.backpressure))
      .killSwitch
      .interruptibleRunWith(Sink.foreach { v =>
        println(s"v=$v")
      })
      .unit
      .race(ZIO.unit.delay(5.seconds))
      .as(ExitCode(0))
  }
}
