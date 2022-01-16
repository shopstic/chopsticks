package dev.chopsticks.fp

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.fp.util.PlatformUtils
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import org.scalatest.{Assertion, BeforeAndAfterAll, Suite}
import zio.console.Console
import zio.{CancelableFuture, ExecutionStrategy, Has, RIO, ZLayer, ZManaged}

trait ZScalatestSuite extends BeforeAndAfterAll {
  this: Suite =>

  protected lazy val hoconConfigLayer: ZLayer[Any, Throwable, HoconConfig] = HoconConfig.live(None)

  protected lazy val bootstrapRuntime: zio.Runtime[Console] = {
    val bootstrapPlatform = PlatformUtils
      .create(
        corePoolSize = 0,
        keepAliveTimeMs = 2000,
        threadPoolName = "zio-test-bootstrap"
      )

    zio.Runtime[Console](Has(Console.Service.live), bootstrapPlatform)
  }

  protected lazy val runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
    val zEnvLayer = zio.ZEnv.live
    val akkaEnvLayer = AkkaEnv.live()
    val izLoggingRouterLayer = IzLoggingRouter.live
    val izLoggingLayer = IzLogging.live()

    (hoconConfigLayer >+> (akkaEnvLayer ++ izLoggingRouterLayer) >+> izLoggingLayer) ++ zEnvLayer
  }

  protected lazy val (appEnv, releaseAppEnv) = bootstrapRuntime.unsafeRun(
    for {
      relMap <- ZManaged.ReleaseMap.make
      env <- runtimeLayer.build.zio.provideSome[Any]((_, relMap)).map(_._2)
    } yield {
      (env, relMap.releaseAll(zio.Exit.Success(()), ExecutionStrategy.Sequential).unit)
    }
  )

  override protected def afterAll(): Unit = {
    bootstrapRuntime.unsafeRun(releaseAppEnv)
  }

  def createRunner[R <: ZAkkaAppEnv](inject: RIO[R, Assertion] => RIO[ZAkkaAppEnv, Assertion])
    : RIO[R, Assertion] => CancelableFuture[Assertion] = {
    (testCode: RIO[R, Assertion]) =>
      {
        val effect = inject(testCode)
          .provide(appEnv)
          .interruptAllChildrenPar

        bootstrapRuntime.unsafeRunToFuture(effect)
      }
  }
}
