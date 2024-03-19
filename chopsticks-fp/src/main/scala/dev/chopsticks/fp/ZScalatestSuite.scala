package dev.chopsticks.fp

import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import org.scalatest.{Assertion, BeforeAndAfterAll, Suite}
import zio.{CancelableFuture, RIO, Scope, Unsafe, ZLayer}

trait ZScalatestSuite extends BeforeAndAfterAll {
  this: Suite =>

  protected lazy val hoconConfigLayer: ZLayer[Any, Throwable, HoconConfig] = HoconConfig.live(None)

  protected lazy val bootstrapRuntime: zio.Runtime[Any] = {
    zio.Runtime.default
  }

  protected lazy val runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
    val PekkoEnvLayer = PekkoEnv.live()
    val izLoggingRouterLayer = IzLoggingRouter.live
    val izLoggingLayer = IzLogging.live()

    val layer1: ZLayer[Any, Throwable, HoconConfig with PekkoEnv with IzLoggingRouter] =
      hoconConfigLayer >+> (PekkoEnvLayer ++ izLoggingRouterLayer)
    layer1 >+> izLoggingLayer
  }

  // todo check if this is enough compared to what we used to have
  protected lazy val (appEnv, releaseAppEnv) = {
    Unsafe.unsafe { implicit unsafe =>
      bootstrapRuntime.unsafe
        .run(
          for {
            scope <- Scope.make
            env <- scope.extend[Any](runtimeLayer.build).provideEnvironment(zio.ZEnvironment(scope))
          } yield (env, scope.close(zio.Exit.unit))
        )
        .getOrThrowFiberFailure()
    }
  }

  override protected def afterAll(): Unit = {
    Unsafe.unsafe { implicit unsafe =>
      bootstrapRuntime.unsafe.run(releaseAppEnv).getOrThrowFiberFailure()
    }
  }

  def createRunner[R <: ZAkkaAppEnv](inject: RIO[R, Assertion] => RIO[ZAkkaAppEnv, Assertion])
    : RIO[R, Assertion] => CancelableFuture[Assertion] = {
    (testCode: RIO[R, Assertion]) =>
      {
        val effect = inject(testCode)
          .provideEnvironment(appEnv)
          .interruptAllChildrenPar

        Unsafe.unsafe { implicit unsafe =>
          bootstrapRuntime.unsafe.runToFuture(effect)
        }
      }
  }
}
