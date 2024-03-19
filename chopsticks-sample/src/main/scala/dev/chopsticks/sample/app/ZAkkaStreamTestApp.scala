package dev.chopsticks.sample.app

import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.stream.ZAkkaFlow
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import dev.chopsticks.stream.ZAkkaGraph._
import zio.{RIO, ZIO}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

object ZAkkaStreamTestApp extends ZPekkoApp {
  override def run: RIO[ZAkkaAppEnv, Any] = {
    val stream = for {
      dispatcher <- PekkoEnv.dispatcher
      runnableGraph <- Source(1 to 10)
        .toZAkkaSource
        .mapAsyncWithScope(10) { (i, scope) =>
          val task =
            ZIO.succeed(println(s"Processing with scope $i")) *>
              ZIO.succeed(i).delay(6.seconds.toJava) *>
              ZIO.succeed(println(s"Finished processing with scope $i"))

          scope
            .fork(task.onInterrupt(ZIO.succeed(println(s"Interrupted with scope $i"))))
            .as(i)
        }
        .mapAsync(10) { i =>
          val task = ZIO.succeed(println(s"Processing $i")) *>
            ZIO.succeed(i).delay(if (i > 5) 6.seconds.toJava else zio.Duration.Zero)
          task.onInterrupt(ZIO.succeed(println(s"Interrupted $i")))
        }
        .viaBuilder(_.take(3).buffer(10, OverflowStrategy.backpressure).throttle(1, 1.second))
        .killSwitch
        .viaBuilderWithScopeMatM { (builder, scope) =>
          for {
            flow <- ZAkkaFlow[Int]
              .mapAsync(1) { i =>
                val task =
                  ZIO.succeed(println(s"Also processing $i")) *> ZIO.succeed(i).delay(1.second.toJava)
                task.onInterrupt(
                  ZIO.succeed(println(s"Interrupting also to $i, going to delay for 3 seconds")) *>
                    ZIO.succeed(println(s"Interrupted also processing $i")).delay(3.seconds.toJava)
                )
              }
              .make(scope)
          } yield {
            builder
              .alsoToMat(
                flow
                  .toMat(Sink.foreach { i =>
                    println(s"Also to: $i")
                  })(Keep.right)
              )(Keep.right)
          }
        }(Keep.both)
        .toMat(Sink.foreach { e =>
          if (e > 2) throw new IllegalStateException("test death")
          println(s"OUT $e")
        }) { case ((ks, f1), f2) =>
          implicit val ec: ExecutionContextExecutor = dispatcher
          ks -> f1.transformWith(_ => f2)
        }
      _ <- runnableGraph.interruptibleRun()
    } yield ()

    val app = stream *> ZIO.succeed(println("Stream completed"))

    app
  }
}
