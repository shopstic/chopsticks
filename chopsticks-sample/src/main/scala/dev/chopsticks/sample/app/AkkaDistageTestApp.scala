package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AkkaDistageApp.DiModule
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDistageApp, DiLayers, ZService}
import zio._
import zio.clock.Clock

object AkkaDistageTestApp extends AkkaDistageApp {
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
        ZManaged.make {
          import zio.duration._

          for {
            foo <- ZIO.access[Foo](_.service)
            zioLogger <- ZIO.access[IzLogging](_.service.zioLogger)
            fib <- zioLogger.info(s"Still going: ${foo.foo}").repeat(Schedule.fixed(1.second)).forkDaemon
          } yield fib
        }(_.interrupt).as(BarService(bar))
      )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override protected def define(akkaAppModuleDef: DiModule, lbConfig: Config): DiModule = {
    akkaAppModuleDef ++ DiLayers(
      IzLogging.live(lbConfig),
      Bar.live("bar"),
      Foo.live("foo"),
      app
    )
  }

  private def doFoo(): URIO[IzLogging, Unit] = {
    ZService[IzLogging.Service].flatMap(_.zioLogger.error("test error here"))
  }

  protected def app: URIO[Clock with IzLogging with AkkaEnv with Bar, Unit] = {
    val effect = for {
      bar <- ZIO.access[Bar](_.service)
      akkaService <- ZIO.access[AkkaEnv](_.service)
      logging <- ZIO.access[IzLogging](_.service)
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
}
