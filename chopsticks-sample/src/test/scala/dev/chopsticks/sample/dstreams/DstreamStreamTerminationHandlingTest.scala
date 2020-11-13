package dev.chopsticks.sample.dstreams

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import dev.chopsticks.dstream.Dstreams
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.sample.app.dstreams.proto.grpc_akka_master_worker.{Assignment, Result}
import dev.chopsticks.sample.app.dstreams.{
  DstreamsSampleMasterApp,
  DstreamsSampleMasterAppConfig,
  DstreamsSampleWorkerApp
}
import zio.{Promise, Schedule, Task, ZIO, ZManaged}
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.TestEnvironment

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamStreamTerminationHandlingTest extends DstreamsDiRunnableSpec {

  override def spec: ZSpec[TestEnvironment, Any] = {
    suite("Dstream stream termination handing")(
      diTestM("Worker should deliver its whole stream even in the presence in the long pause on the master side") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            serverProbeFork <- {
              val assignment = Assignment(1)
              Dstreams
                .distribute(dstreamState)(ZIO.succeed(assignment)) { work =>
                  Task(work.source.runWith(TestSink.probe[Result]))
                }
                .fork
            }
            clientFork <- {
              Dstreams
                .work(grpcClient.doWork()) { _ =>
                  Task {
                    Source(List(1, 2, 3)).map(x => Result(Result.Body.Value(x.toLong)))
                  }
                }
                .fork
            }
            serverProbe <- serverProbeFork.join
            _ <- Task {
              serverProbe
                .requestNext(Result(Result.Body.Value(1L)))
                .requestNext(Result(Result.Body.Value(2L)))
            }
            _ <- ZIO.sleep(250.millis.toJava)
            _ <- Task(serverProbe.requestNext(Result(Result.Body.Value(3L))))
            _ <- Task(serverProbe.expectComplete())
            _ <- clientFork.join
          } yield assert(())(anything)
        }
        managedZio.use(identity)
      } @@ timeout(10.seconds.toJava),
      diTestM("Master should see canceled stream when worker fails its stream") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            serverProbeFork <- {
              val assignment = Assignment(1)
              Dstreams
                .distribute(dstreamState)(ZIO.succeed(assignment)) { work =>
                  Task(work.source.runWith(TestSink.probe[Result]))
                }
                .fork
            }
            promiseProbe <- Promise.make[Nothing, TestPublisher.Probe[Result]]
            _ <- {
              Dstreams
                .work(grpcClient.doWork()) { _ =>
                  Task(TestSource.probe[Result].preMaterialize())
                    .tap { case (probe, _) => promiseProbe.succeed(probe) }
                    .map { case (_, source) => source }
                }
                .fork
            }
            serverProbe <- serverProbeFork.join
            probe <- promiseProbe.await
            _ <- Task(probe.sendNext(Result(Result.Body.Value(1L))))
            _ <- Task(probe.sendNext(Result(Result.Body.Value(2L))))
            _ <- Task {
              serverProbe
                .requestNext(Result(Result.Body.Value(1L)))
                .requestNext(Result(Result.Body.Value(2L)))
            }
            _ <- Task(probe.sendError(new RuntimeException("Failing worker source")))
            error <- Task(serverProbe.expectError())
          } yield assert(error)(
            hasMessage(matchesRegex("""Stream with ID \[\d+\] was closed by peer with code CANCEL\(0x08\)"""))
          )
        }
        managedZio.use(identity)
      } @@ timeout(10.seconds.toJava),
      diTestM("Master should be able to collect only a part of a worker's source") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            serverProbeFork <- {
              val assignment = Assignment(1)
              Dstreams
                .distribute(dstreamState)(ZIO.succeed(assignment)) { work =>
                  Task(work.source.runWith(TestSink.probe[Result]))
                }
                .fork
            }
            workerFork <- {
              Dstreams
                .work(grpcClient.doWork()) { _ =>
                  Task(Source.repeat(Result(Result.Body.Value(1L))))
                }
                .fork
            }
            serverProbe <- serverProbeFork.join
            _ <- Task {
              serverProbe
                .requestNext(Result(Result.Body.Value(1L)))
                .requestNext(Result(Result.Body.Value(1L)))
                .cancel()
            }

            // join worker to make sure it has completed
            _ <- workerFork.join
          } yield assert(())(anything)
        }
        managedZio.use(identity)
      } @@ timeout(10.seconds.toJava),
      diTestM("Master should disconnect worker if it didn't receive any message within specified idleTimeout") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            serverProbeFork <- {
              Dstreams
                .distribute(dstreamState)(ZIO.succeed(Assignment(1))) { work =>
                  Task(work.source.runWith(TestSink.probe[Result]))
                }
                .fork
            }
            workerFork <- {
              Dstreams
                .work(grpcClient.doWork()) { _ =>
                  Task(Source.single(Result(Result.Body.Value(1L))).delay(1.second))
                }
                .fork
            }
            serverProbe <- serverProbeFork.join
            serverError <- Task(serverProbe.expectSubscriptionAndError())
            _ <- Task(workerFork.join)
          } yield {
            assert(serverError)(hasMessage(equalsIgnoreCase(
              "The HTTP/2 connection was shut down while the request was still ongoing"
            )))
          }
        }
        managedZio
          .use(identity)
          .updateService[DstreamsSampleMasterAppConfig](conf => conf.copy(idleTimeout = 150.millis))
      } @@ timeout(10.seconds.toJava),
      diTestM("Worker should disconnect itself if it didn't receive assignment within specified initialTimeout") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            _ <- {
              Dstreams
                .distribute(dstreamState)(ZIO.succeed(Assignment(1)).delay(500.millis.toJava)) { work =>
                  Task.fromFuture(_ => work.source.runWith(Sink.seq))
                }
                .fork
            }
            clientError <- {
              Dstreams
                .work(grpcClient.doWork(), 100.millis, Schedule.stop) { _ =>
                  Task(Source.repeat(Result(Result.Body.Value(1L))))
                }
                .as(new RuntimeException("Expected client to fail, it succeeded instead"))
                .flip
            }
          } yield assert(clientError)(isSubtype[TimeoutException](anything))
        }
        managedZio.use(identity)
      } @@ timeout(10.seconds.toJava),
      diTestM("Worker should keep retring by default if it didn't receive assignment within specified initialTimeout") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          grpcClient <- DstreamsSampleWorkerApp.manageClient(serverBinding.localAddress.getPort)
          akkaService <- ZManaged.access[AkkaEnv](_.get)
        } yield {
          import akkaService.actorSystem
          for {
            assignmentPromise <- Promise.make[Nothing, Assignment]
            serverFork <- {
              Dstreams
                .distribute(dstreamState)(assignmentPromise.await) { work =>
                  Task.fromFuture(_ => work.source.take(2).runWith(Sink.seq))
                }
                // due to unfortunate timings in this test
                //there may be a situation when distribute fails and needs to be retried
                .retry(Schedule.forever)
                .fork
            }
            _ <- ZIO.succeed(Assignment(1)).delay(500.millis.toJava).flatMap(assignmentPromise.succeed).fork
            _ <- {
              val outSource = Source.single(Result(Result.Body.Value(1L)))
              Dstreams.work(grpcClient.doWork(), 200.millis)(_ => Task(outSource))
            }
            _ <- serverFork.join
          } yield assert(())(anything)
        }
        managedZio.use(identity)
      } @@ timeout(10.seconds.toJava)
    )
  }

}
