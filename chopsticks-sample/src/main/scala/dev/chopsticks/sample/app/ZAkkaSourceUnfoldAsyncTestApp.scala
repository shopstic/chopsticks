package dev.chopsticks.sample.app

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource
import zio.{ExitCode, RIO, UIO, ZIO}
import zio.duration._

object ZAkkaSourceUnfoldAsyncTestApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    ZAkkaSource
      .unfoldAsync(1) { v =>
        ZIO.some((v * 2, v)).delay(1.second).onInterrupt(_ =>
          UIO(println(s"Got interrupted v=$v going to delay")) *> UIO(println("Now release")).delay(2.seconds)
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
