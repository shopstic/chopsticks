package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.{AkkaDistageApp, ZLogger, ZService}
import dev.chopsticks.sample.app.AkkaDistageTestApp.Bar.BarService
import dev.chopsticks.sample.app.AkkaDistageTestApp.Foo.FooService
import distage.ModuleDef
import zio._

object AkkaDistageTestApp extends AkkaDistageApp {
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
  override protected def define(
    akkaEnv: Layer[Nothing, AkkaEnv],
    lbConfig: Config
  ): ModuleDef = {
    new ModuleDef {
      make[LogEnv.Service].fromHas(LogEnv.live)
      make[AkkaEnv.Service].fromHas(akkaEnv)
      make[Foo.Service].fromValue(FooService("foo"))
      make[Bar.Service].fromHas(ZLayer.succeed(BarService("foo")))
      make[Unit].fromHas(app)
    }
  }

  def app: ZIO[LogEnv with AkkaEnv with Has[Bar.Service] with Has[Foo.Service], Nothing, Unit] = {
    for {
      foo <- ZService[Foo.Service]
      bar <- ZService[Bar.Service]
      akkaService <- ZService[AkkaEnv.Service]
      _ <- ZLogger.info(s"foo=${foo.foo} bar=${bar.bar} as=${akkaService.actorSystem.startTime}")
    } yield ()
  }
}
