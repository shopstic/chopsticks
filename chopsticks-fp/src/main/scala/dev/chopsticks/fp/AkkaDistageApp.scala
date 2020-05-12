package dev.chopsticks.fp

import java.nio.file.Paths

import akka.Done
import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import distage.{Injector, ModuleDef}
import izumi.distage.model.plan.GCMode
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio.internal.tracing.TracingConfig
import zio.{Layer, Task, UIO, ZIO}

import scala.util.Try
import scala.util.control.NonFatal

trait AkkaDistageApp extends LoggingContext {
  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def define(akkaEnv: Layer[Nothing, AkkaEnv], lbConfig: Config): ModuleDef

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replace("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replace(".", "/") + "/" + appName
    val customAppConfig = scala.sys.props.get("config.file") match {
      case Some(customConfigFile) =>
        ConfigFactory
          .parseFile(Paths.get(customConfigFile).toFile, ConfigParseOptions.defaults().setAllowMissing(false))
          .resolve(ConfigResolveOptions.defaults())
      case None =>
        ConfigFactory.empty()
    }

    val config = customAppConfig.withFallback(
      ConfigFactory.load(
        appConfigName,
        ConfigParseOptions.defaults.setAllowMissing(false),
        ConfigResolveOptions.defaults
      )
    )

    if (!config.getBoolean("akka.coordinated-shutdown.run-by-jvm-shutdown-hook")) {
      throw new IllegalArgumentException(
        "'akka.coordinated-shutdown.run-by-jvm-shutdown-hook' is not set to 'on'. Check your HOCON application config."
      )
    }

    val akkaActorSystem = createActorSystem(appName, config)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)

    val akkaEnvLayer = AkkaEnv.live(akkaActorSystem)
    val appModuleDef = define(akkaEnvLayer, config)

    val injector = Injector()

    val app = injector.plan(appModuleDef, GCMode.NoGC).assertImportsResolved match {
      case Left(exception) =>
        UIO {
          Console.err.println(exception.getMessage)
          val _ = shutdown.run(JvmExitReason)
        }
      case Right(plan) =>
        injector.produceF[Task](plan).use(_ => Task.unit)
    }

    val zioTracingEnabled = Try(config.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)

    val runtime = AkkaApp.createRuntime(
      akkaEnvLayer ++ LogEnv.live,
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )

    val main = for {
      appFib <- app
        .ensuring(ZIO.interruptAllChildren)
        .fork
      _ <- UIO {
        shutdown.addTask("app-interruption", "interrupt app") { () =>
          runtime.unsafeRunToFuture(appFib.interrupt.ignore *> UIO(Done))
        }
      }
      result <- appFib.join
    } yield result

    try {
      val _ = runtime.unsafeRun(main)
      sys.exit(0)
    }
    catch {
      case NonFatal(e) =>
        runtime.platform.reportFailure(Die(e))
    }
  }
}
