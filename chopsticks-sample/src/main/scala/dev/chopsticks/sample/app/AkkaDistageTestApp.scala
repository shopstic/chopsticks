package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.{AkkaDistageApp, ZService}
import dev.chopsticks.sample.app.AkkaDistageTestApp.Bar.BarService
import dev.chopsticks.sample.app.AkkaDistageTestApp.Foo.FooService
import distage.{Injector, ModuleDef}
import izumi.distage.model.definition.DIResource.DIResourceBase
import zio._
import zio.clock.Clock

object AkkaDistageTestApp extends AkkaDistageApp {
  type Env = Clock with IzLogging with AkkaEnv with Has[Bar.Service]

  object Foo {
    trait Service {
      def foo: String
    }
    final case class FooService(foo: String) extends Service
  }

  object Bar {
    trait Service {
      def bar: String
    }
    final case class BarService(bar: String) extends Service
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override protected def defineEnv(
    akkaEnv: Layer[Nothing, AkkaEnv],
    lbConfig: Config
  ): DIResourceBase[Task, Env] = {
    val barLayer: ZLayer[Has[Foo.Service], Nothing, Has[Bar.Service]] =
      ZLayer.requires[Has[Foo.Service]] >>> ZLayer.succeed(BarService("foo"))

    val module = new ModuleDef {
      make[IzLogging.Service].fromHas(IzLogging.live(lbConfig))
      make[AkkaEnv.Service].fromHas(akkaEnv)
      make[Clock.Service].fromHas(Clock.live)
      make[Bar.Service].fromHas(barLayer)
      make[Foo.Service].fromValue(FooService("foo"))
      make[Env].fromHas(ZIO.environment[Env])
    }

    Injector().produceGetF[Task, Env](module)
  }

  private def doFoo(): ZIO[IzLogging, Nothing, Unit] = {
    ZService[IzLogging.Service].flatMap(_.zioLogger.error("test error here"))
  }

  override protected def run: ZIO[Env, Throwable, Unit] = {
    val effect = for {
      bar <- ZService[Bar.Service]
      akkaService <- ZService[AkkaEnv.Service]
      logging <- ZService[IzLogging.Service]
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
