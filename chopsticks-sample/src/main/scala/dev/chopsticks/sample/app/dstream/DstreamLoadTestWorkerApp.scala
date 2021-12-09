package dev.chopsticks.sample.app.dstream

import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Keep, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.Dstreams
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.sample.app.dstream.proto.load_test._
import dev.chopsticks.util.config.PureconfigLoader
import io.grpc.{Status, StatusRuntimeException}
import pureconfig.ConfigConvert
import zio._

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NoStackTrace

final case class WorkersConfig(
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

final case class DstreamLoadTestWorkerAppConfig(
  serverPort: Int,
  workers: WorkersConfig
)

object DstreamLoadTestWorkerAppConfig {
  // noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[DstreamLoadTestWorkerAppConfig]
  }
}

object DstreamLoadTestWorkerApp extends AkkaDiApp[DstreamLoadTestWorkerAppConfig] {
  final case object RandomFailureTestException
      extends RuntimeException("Failed randomly for testing...")
      with NoStackTrace

  override def config(allConfig: Config): Task[DstreamLoadTestWorkerAppConfig] = {
    Task(PureconfigLoader.unsafeLoad[DstreamLoadTestWorkerAppConfig](allConfig, "app"))
  }

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: DstreamLoadTestWorkerAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(akkaAppDi ++ DiLayers(
        ZLayer.succeed(appConfig),
        AppLayer(app)
      ))
    }
  }

  // noinspection TypeAnnotation
  def app = {
    val managed = for {
      appConfig <- ZManaged.access[AppConfig](_.get)
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

    managed.use(identity)
  }

  private[sample] def manageClient(port: Int) = {
    Dstreams
      .manageClient(ZIO.access[AkkaEnv](_.get).map { env =>
        import env.actorSystem
        StreamMasterClient(
          GrpcClientSettings
            .connectToServiceAt("localhost", port)
            .withTls(false)
        )
      })
  }

  private[sample] def runWorker(
    port: Int,
    i: Int,
    willCrash: Boolean,
    willFail: Boolean
  ) = {
    val workerId = s"worker-$i"

    manageClient(port)
      .use { client =>
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
                      throw RandomFailureTestException
                    }

                    if (willFail && iteration == assignment.iteration) Result(body = Result.Body.ErrorCode(123))
                    else Result(body = Result.Body.Value(sum))
                  }
                  .watchTermination()(Keep.right)
                  .preMaterialize()
              }
              _ <- Task
                .fromFuture(_ => futureDone)
                .log(s"$workerId processing $assignment", logTraceOnError = false)
                .forkDaemon
            } yield source
          }
          .log(s"Client: running $workerId", logTraceOnError = false)
      }
  }

}
