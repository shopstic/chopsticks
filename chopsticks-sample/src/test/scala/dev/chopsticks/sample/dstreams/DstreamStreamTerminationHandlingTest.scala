package dev.chopsticks.sample.dstreams

import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import dev.chopsticks.dstream.Dstreams
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.sample.app.dstreams.proto.grpc_akka_master_worker.{Assignment, Result}
import dev.chopsticks.sample.app.dstreams.{DstreamsSampleMasterApp, DstreamsSampleWorkerApp}
import zio.{Promise, Task, ZIO, ZManaged}
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.TestEnvironment

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
          } yield assert(error)(hasMessage(matchesRegex("""Stream with ID \[\d+\] was closed by peer with code CANCEL\(0x08\)""")))
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
      } @@ timeout(10.seconds.toJava)
    )
  }

}
