package dev.chopsticks.sample.app.dstreams

import akka.Done
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Keep, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.Dstreams
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, ZIOExtensions}
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.sample.app.dstreams.proto.grpc_akka_master_worker._
import dev.chopsticks.util.config.PureconfigLoader
import io.grpc.{Status, StatusRuntimeException}
import pureconfig.ConfigConvert
import zio._

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

final case class DstreamsSampleWorkersConfig(
  count: Option[Int],
  retryInterval: FiniteDuration,
  crashProbability: Double,
  failureProbability: Double
) {
  assert(crashProbability >= 0 && crashProbability <= 1, "crashProbability must be between 0 and 1")
  assert(failureProbability >= 0 && crashProbability <= 1, "failureProbability must be between 0 and 1")

  def resolvedCount: Int =
    Math.max(
      count.getOrElse(java.lang.Runtime.getRuntime.availableProcessors() - 4),
      1
    )
}

final case class DstreamsSampleWorkerAppConfig(
  serverPort: Int,
  workers: DstreamsSampleWorkersConfig
)

object DstreamsSampleWorkerAppConfig {
  //noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[DstreamsSampleWorkerAppConfig]
  }
}

object DstreamsSampleWorkerApp extends AkkaDiApp[DstreamsSampleWorkerAppConfig] {

  override def config(allConfig: Config): Task[DstreamsSampleWorkerAppConfig] = {
    Task(PureconfigLoader.unsafeLoad[DstreamsSampleWorkerAppConfig](allConfig, "app"))
  }

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: DstreamsSampleWorkerAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        ZLayer.succeed(appConfig),
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  def app: ZIO[AkkaEnv with MeasuredLogging with AppConfig, StatusRuntimeException, Unit] = {
    val managedZio = for {
      appConfig <- ZManaged.service[DstreamsSampleWorkerAppConfig]
    } yield {
      for {
        _ <- ZIO.foreachPar_(1 to appConfig.workers.resolvedCount) { i =>
          val willCrash = UIO(Math.random() < appConfig.workers.crashProbability)
          val willFail = UIO(Math.random() < appConfig.workers.failureProbability)
          willCrash
            .zip(willFail)
            .flatMap { case (crash, fail) =>
              runWorker(appConfig.serverPort, i, crash, fail)
                .foldM(
                  {
                    case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
                      ZIO.fail(e)
                    case e =>
                      ZIO.left(e)
                  },
                  r => ZIO.right(r)
                )
            }
            .repeat(Schedule.fixed(appConfig.workers.retryInterval.toJava))
        }
      } yield ()
    }
    managedZio.use(identity)
  }

  def manageClient(port: Int): URManaged[AkkaEnv with MeasuredLogging, StreamMasterClient] = {
    Dstreams.createManagedClient(ZIO.access[AkkaEnv](_.get).map { env =>
      import env.actorSystem
      StreamMasterClient(
        GrpcClientSettings
          .connectToServiceAt("localhost", port)
          .withTls(false)
      )
    })
  }

  def runWorker(
    port: Int,
    i: Int,
    willCrash: Boolean,
    willFail: Boolean
  ): RIO[AkkaEnv with IzLogging with MeasuredLogging, Done] = {
    val workerId = s"worker-$i"

    manageClient(port).use { client =>
      Dstreams
        .work(client.doWork().addHeader(Dstreams.WORKER_ID_HEADER, workerId)) { assignment =>
          for {
            akkaService <- ZIO.access[AkkaEnv](_.get)
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
        .log(s"Client: running $workerId")
    }
  }

}
