package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers, ZService}
import zio._
import zio.clock.Clock

object AkkaDiTestApp extends AkkaDiApp[Unit] {
  type Foo = Has[Foo.Service]

  object Foo {
    trait Service {
      def foo: String
    }
    final case class FooService(foo: String) extends Service

    def live(foo: String): Layer[Nothing, Foo] = ZLayer.succeed(FooService(foo))
  }

  type Bar = Has[Bar.Service]

  object Bar {
    trait Service {
      def bar: String
    }
    final case class BarService(bar: String) extends Service

    def live(bar: String): URLayer[Clock with IzLogging with Foo, Bar] = {
      ZLayer.fromManaged(
        ZManaged.makeInterruptible {
          import zio.duration._

          for {
            foo <- ZIO.access[Foo](_.get)
            zioLogger <- ZIO.access[IzLogging](_.get.zioLogger)
            fib <- zioLogger.info(s"Still going: ${foo.foo}").repeat(Schedule.fixed(1.second)).forkDaemon
          } yield fib
        } { fib =>
          UIO(println("INTERRUPTING===================")) *>
            fib.interrupt
        }.as(BarService(bar))
      )
    }
  }

  private def doFoo(): URIO[IzLogging, Unit] = {
    ZService[IzLogging.Service].flatMap(_.zioLogger.error("test error here"))
  }

  def app = {
    val effect = for {
      bar <- ZIO.access[Bar](_.get)
      akkaService <- ZIO.access[AkkaEnv](_.get)
      logging <- ZIO.access[IzLogging](_.get)
      zioLogger = logging.zioLogger.withCustomContext("userId" -> "user@google.com", "company" -> "acme")
      logger = logging.logger
      _ <- zioLogger.info(s"${bar.bar} ${akkaService.actorSystem.startTime}")
      _ <- doFoo()
    } yield {
      logger.info("From sync logger here")
    }

    import zio.duration._
    effect.repeat(Schedule.fixed(1.second)).unit
  }

  override def liveEnv(akkaAppDi: DiModule, appConfig: Unit, allConfig: Config): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          IzLogging.live(allConfig),
          Bar.live("bar"),
          Foo.live("foo"),
          AppLayer(app)
        )
      )
    }
  }

  override def config(allConfig: Config): Task[Unit] = Task.unit
}
