package dev.chopsticks.sample.app

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import com.typesafe.config.Config
import dev.chopsticks.fp._
import dev.chopsticks.util.config.PureconfigLoader
import zio.duration._
import zio.{Schedule, Task, ZIO, ZManaged}
import dev.chopsticks.fp.zio_ext._

object PlainSampleApp extends AkkaApp {
  final case class AppConfig(foo: Int, bar: String)

  type Cfg = ConfigEnv[AppConfig]
  type Env = AkkaApp.Env with Cfg

  protected def createEnv(rawConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env] = {
    import dev.chopsticks.util.config.PureconfigConverters._

    ZManaged
      .environment[AkkaApp.Env]
      .map { akkaEnv =>
        new AkkaApp.LiveEnv with Cfg {
          val akkaService: AkkaEnv.Service = akkaEnv.akkaService
          val config: AppConfig = PureconfigLoader.unsafeLoad[AppConfig](rawConfig, "app")
        }
      }
  }

  def run: ZIO[Env, Throwable, Unit] = {
    for {
      config <- ZIO.access[Cfg](_.config)
      logEnv <- ZIO.environment[LogEnv]
      _ <- ZLogger.info(s"Works config=$config")
      _ <- ZIO
        .effectSuspend {
          val delay = ThreadLocalRandom.current().nextLong(100, 700)
          val duration = zio.duration.Duration(delay, TimeUnit.MILLISECONDS)
          println(s"""Going to delay: $duration""")
          Task
            .fail(new IllegalStateException("Test failure"))
            .delay(duration)
        }
        .retryForever(
          retryPolicy = (Schedule.exponential(500.millis) || Schedule.spaced(4.seconds)).onDecision { (_: Any, d) =>
            ZLogger.info(s"Retry backoff: $d").provide(logEnv)
          },
          repeatSchedule = Schedule.forever.tapOutput(i => ZLogger.info(s"Success $i")),
          retryResetMinDuration = 750.millis
        )
    } yield ()
  }
}
