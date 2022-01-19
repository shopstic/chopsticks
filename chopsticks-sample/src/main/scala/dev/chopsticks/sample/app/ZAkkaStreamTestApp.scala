package dev.chopsticks.sample.app

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.stream.ZAkkaFlow
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import dev.chopsticks.stream.ZAkkaGraph._
import zio.{ExitCode, RIO, UIO, ZIO}

import scala.concurrent.ExecutionContextExecutor

object ZAkkaStreamTestApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val stream = for {
      dispatcher <- AkkaEnv.dispatcher
      runnableGraph <- Source(1 to 10)
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
        .viaBuilderWithScopeMatM { (builder, scope) =>
          for {
            flow <- ZAkkaFlow[Int]
              .mapAsync(1) { i =>
                import zio.duration._
                val task =
                  UIO(println(s"Also processing $i")) *> ZIO.succeed(i).delay(1.second)
                task.onInterrupt(UIO(println(s"Interrupting also to $i, going to delay for 3 seconds")) *> UIO(
                  println(s"Interrupted also processing $i")
                ).delay(3.seconds))
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

    val app = stream *> UIO(
      println("Stream completed")
    )

    app
      .as(ExitCode(0))
  }
}
