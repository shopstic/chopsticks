package dev.chopsticks.sample.app

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.zio_ext.TaskExtensions
import zio.duration._
import zio.{Exit, ExitCode, Task, UIO, URIO, ZScope}

import scala.concurrent.Future

object DeleteMeApp extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    implicit val as = ActorSystem()

    implicit val rt: zio.Runtime[zio.ZEnv] = zio.Runtime.default
    val zScope = rt.unsafeRun(ZScope.make[Exit[Any, Any]])

    val run = Task
      .fromFuture { implicit ec =>
        Source(1 to 10)
          .mapAsyncUnordered(10) { i =>
            if (i == 10) Future.failed(new IllegalStateException("bad"))
            else {
              val task = UIO(println(s"Before process $i")) *> UIO(println(s"Process $i")).delay(3.seconds) *> UIO(
                println(s"After process $i")
              ).as(i)

              val fib = rt.unsafeRun(task.onInterrupt(UIO(println(s"Task $i is interrupted"))).forkIn(zScope.scope))
              rt.unsafeRunToFuture(fib.join)
            }
          }
          .toMat(akka.stream.scaladsl.Sink.foreach(println)) { (_, future) =>
            future
              .transformWith { t =>
                println(s"stream completed with $t")
                (zScope.close(Exit.Success(())).tap(v => UIO(println(s"after scope closing $v")))).unsafeRunToFuture
              }
          }
          .run()
      }

    run.as(ExitCode(0)).orDie
  }
}
