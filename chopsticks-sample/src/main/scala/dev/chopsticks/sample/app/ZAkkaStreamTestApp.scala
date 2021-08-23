package dev.chopsticks.sample.app

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import zio.{ExitCode, RIO, UIO, ZIO}

object ZAkkaStreamTestApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val stream = Source(1 to 10)
      .toZAkkaSource
      .mapAsyncWithScope(10) { (i, scope) =>
        import zio.duration._
        val task =
          UIO(println(s"Processing with scope $i")) *> ZIO.succeed(i).delay(6.seconds) *> UIO(
            println(s"Finished processing with scope $i")
          )

        scope
          .fork(task.onInterrupt(UIO(println(s"Interrupted with scope $i"))))
          .as(i)
      }
      .mapAsync(10) { i =>
        import zio.duration._
        val task = UIO(println(s"Processing $i")) *> ZIO.succeed(i).delay(if (i > 5) 6.seconds else Duration.Zero)
        task.onInterrupt(UIO(println(s"Interrupted $i")))
      }
      .viaBuilder(_.take(3).buffer(10, OverflowStrategy.backpressure).throttle(
        1, {
          import scala.concurrent.duration._
          1.second
        }
      ))
      .killSwitch
      .interruptibleRunWith(akka.stream.scaladsl.Sink.foreach { e =>
        if (e > 2) throw new IllegalStateException("test death")
        println(s"OUT $e")
      }) *> UIO(
      println("Stream completed")
    )

    stream
      .as(ExitCode(0))
  }
}
