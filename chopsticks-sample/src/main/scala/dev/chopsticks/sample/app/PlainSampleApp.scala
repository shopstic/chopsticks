package dev.chopsticks.sample.app

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import com.typesafe.config.Config
import dev.chopsticks.fp._
import dev.chopsticks.util.config.PureconfigLoader
import zio.duration._
import zio.{Task, ZIO, ZManaged, ZSchedule}
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
          val akka: AkkaEnv.Service = akkaEnv.akka
          val config: AppConfig = PureconfigLoader.unsafeLoad[AppConfig](rawConfig, "app")
        }
      }
  }

  protected def run: ZIO[Env, Throwable, Unit] = {
    for {
      config <- ZIO.access[Cfg](_.config)
      logEnv <- ZIO.environment[LogEnv]
      _ <- ZLogger.info(s"Works config=$config")
      _ <- ZIO
        .effectSuspend {
          val delay = ThreadLocalRandom.current().nextLong(100, 1500)
          val duration = zio.duration.Duration(delay, TimeUnit.MILLISECONDS)
          println(s"""Going to delay: $duration""")
          Task
            .fail(new IllegalStateException("Test failure"))
            .delay(duration)
        }
        .retryForever(
          retryPolicy = (ZSchedule.exponential(500.millis) || ZSchedule.spaced(4.seconds)).onDecision { (_: Any, d) =>
            ZLogger.info(s"Retry backoff: ${d.delay}").provide(logEnv)
          },
          repeatSchedule = ZSchedule.forever.logOutput(i => ZLogger.info(s"Success $i")),
          retryResetMinDuration = 750.millis
        )
    } yield ()
  }
}
