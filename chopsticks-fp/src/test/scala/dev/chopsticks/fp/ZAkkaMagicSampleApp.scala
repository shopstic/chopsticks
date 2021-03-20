package dev.chopsticks.fp

import com.typesafe.config.{Config, ConfigFactory}
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.{HoconConfig, TypedConfig}
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.iz_logging.IzLogging.IzLoggingConfig
import logstage.Log
import pureconfig.ConfigConvert
import zio.magic._
import zio.{ExitCode, Has, RIO, Schedule, UIO, ZIO, ZLayer}

import java.time.LocalDateTime
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object ZAkkaMagicSampleApp extends ZAkkaApp {
  final case class AppConfig(foo: String, bar: Boolean, baz: FiniteDuration)
  object AppConfig {
    //noinspection TypeAnnotation
    implicit lazy val configConvert = {
      import dev.chopsticks.util.config.PureconfigConverters._
      ConfigConvert[AppConfig]
    }
  }

  // Demo manually supplying runtimeLayer
  override def runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
    val customHoconConfig: ZLayer[Any, Throwable, Has[HoconConfig.Service]] =
      HoconConfig.live(Some(getClass)).map { cfg =>
        Has(new HoconConfig.Service {
          override val config: Config = ConfigFactory.parseString(
            """
            |app.baz = 123 seconds
            |""".stripMargin
          ).withFallback(cfg.get.config)
        })
      }

    ZLayer.fromMagic[ZAkkaAppEnv](
      zio.ZEnv.live,
      customHoconConfig,
      IzLogging.live(IzLoggingConfig(
        coloredOutput = true,
        level = Log.Level.Debug,
        jsonFileSink = None
      )),
      AkkaEnv.live()
    )
  }

  //noinspection TypeAnnotation
  def app = {
    for {
      as <- AkkaEnv.actorSystem
      zlogger <- IzLogging.zioLogger
      logger <- IzLogging.logger
      appConfig <- TypedConfig.get[AppConfig]
      number <- ZIO.service[Long]
      _ <- zlogger.info(s"$number")
      _ <- zlogger.info(s"actorSystem: $as")
      _ <- zio.console.putStrLn(s"from console here: $appConfig")
      _ <- zlogger.info(s"current thread: ${Thread.currentThread().getName}")
      _ <- zio.blocking.effectBlocking(logger.info(s"current time is: ${LocalDateTime.now}")).repeat(
        Schedule.fixed(1.second.toJava).whileOutput(_ < 5)
      )
    } yield ()
  }

  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val typed = TypedConfig.live[AppConfig]()

    app
      .onInterrupt(UIO(println("app is interrupted, delaying...")) *> UIO(
        println("ok gonna let go now...")
      ).delay(2.seconds.toJava))
      .as(ExitCode(0)).provideSomeMagicLayer[ZAkkaAppEnv](
        typed, {
          println("bug in zio-magic, this is evaluated multiple times")
          ZLayer.succeed(2L)
        }
      )
  }
}
