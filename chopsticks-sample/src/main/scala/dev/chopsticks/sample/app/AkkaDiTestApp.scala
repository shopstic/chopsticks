package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers, ZService}
import org.slf4j.LoggerFactory
import zio._
import zio.clock.Clock

object AkkaDiTestApp extends AkkaDiApp[Unit] {
  private val classicLogger = LoggerFactory.getLogger("classic")

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
            foo <- ZIO.access[Foo](_.get)
            zioLogger <- ZIO.access[IzLogging](_.get.zioLogger)
            fib <- zioLogger.info(s"Still going: ${foo.foo}").repeat(Schedule.fixed(1.second)).interruptible.forkDaemon
          } yield fib
        } { fib =>
          UIO(println("Interrupting daemon fiber")) *>
            fib.interrupt.ignore *> UIO(println("Interrupted daemon fiber"))
        }.as(BarService(bar))
      )
    }
  }

  private def doFoo(): URIO[IzLogging, Unit] = {
    ZService[IzLogging.Service].flatMap(_.zioLogger.error("test error here"))
  }

  //noinspection TypeAnnotation
  def app = {
    val managed = ZManaged.make(UIO("whatever")) { _ =>
      UIO(println("Test long release...")) *> ZIO.unit.delay(java.time.Duration.ofSeconds(5)) *> UIO(
        println("Long release completed")
      )
    }

    val effect = for {
      bar <- ZIO.access[Bar](_.get)
      akkaService <- ZIO.access[AkkaEnv](_.get)
      logging <- ZIO.access[IzLogging](_.get)
      zioLogger = logging.zioLogger.withCustomContext("userId" -> "user@google.com", "company" -> "acme")
      logger = logging.logger
      _ <- zioLogger.info(s"${bar.bar} ${akkaService.actorSystem.startTime}")
      _ <- doFoo()
    } yield {
      classicLogger.info("From classic logger")
      classicLogger.error("Test error from classic logger", new RuntimeException("Test exception"))
      logger.info("From sync logger here")
    }

    managed.use { _ =>
      import zio.duration._
      effect.repeat(Schedule.fixed(1.second)).unit
    }
  }

  override def liveEnv(akkaAppDi: DiModule, appConfig: Unit, allConfig: Config): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          Bar.live("bar"),
          Foo.live("foo"),
          AppLayer(app)
        )
      )
    }
  }

  override def config(allConfig: Config): Task[Unit] = Task.unit
}
