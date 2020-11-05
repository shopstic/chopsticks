package dev.chopsticks.sample.app.dstreams

import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Keep, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.Dstreams
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.sample.app.dstreams.proto.grpc_akka_master_worker.{Result, StreamMasterClient}
import io.grpc.{Status, StatusRuntimeException}
import zio.{Schedule, Task, UIO, ZIO}

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object GrpcAkkaWorkerApp extends AkkaDiApp[Unit] {

  val workerCount = Math.max(
    sys.env.get("WORKER_COUNT").map(_.toInt).getOrElse(Runtime.getRuntime.availableProcessors() - 4),
    1
  )

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Unit,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  private def app = {
    val managedClient = Dstreams.createManagedClient(ZIO.access[AkkaEnv](_.get).map { env =>
      import env.actorSystem
      StreamMasterClient(
        GrpcClientSettings
          .connectToServiceAt("localhost", GrpcAkkaMasterApp.port)
          .withTls(false)
      )
    })

    managedClient.use { client =>
      for {
        logger <- ZIO.access[IzLogging](_.get).map(_.logger)
        _ <- ZIO.foreachPar_(1 to workerCount) { i =>
          runWorker(client, i)
            .tapError(e => UIO(logger.error(s"Worker $i failed with: $e")))
            .foldM(
              {
                case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
                  ZIO.fail(e)
                case e =>
                  ZIO.left(e)
              },
              r => ZIO.right(r)
            )
            .repeat(Schedule.fixed(250.millis.toJava))
        }
      } yield ()
    }
  }

  private def runWorker(client: StreamMasterClient, i: Int) = {
    val workerId = s"worker-$i"
    Dstreams.work(client.doWork().addHeader(Dstreams.WORKER_ID_HEADER, workerId)) { assignment =>
      for {
        akkaService <- ZIO.access[AkkaEnv](_.get)
        willCrash <- UIO(Math.random() > 0.9)
        willFail <- UIO(Math.random() > 0.9)
        (futureDone, source) <- Task {
          import akkaService.actorSystem
          Source(1 to assignment.iteration)
            .map { iteration =>
              var sum = 0L
              for (i <- assignment.from to assignment.to) {
                sum += i + assignment.addition + iteration
              }
              if (willCrash && iteration == assignment.iteration) {
                throw new RuntimeException("Failed randomly for testing...")
              }

              if (willFail && iteration == assignment.iteration) Result(body = Result.Body.ErrorCode(123))
              else Result(body = Result.Body.Value(sum))
            }
            .watchTermination()(Keep.right)
            .preMaterialize()
        }
        _ <- Task
          .fromFuture(_ => futureDone)
          .log(s"$workerId processing $assignment")
          .forkDaemon
      } yield source
    }
  }

}
