package dev.chopsticks.fp

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.util.config.PureconfigLoader
import kamon.Kamon
import kamon.system.SystemMetrics
import pureconfig.{ConfigReader, KebabCase, PascalCase}
import scalaz.zio
import scalaz.zio.internal.{Platform, PlatformLive}
import scalaz.zio.{IO, TaskR, UIO}

trait ConfigurableAkkaApp extends LoggingContext {
  type Env <: AkkaApp.Env
  type AppConfig <: Product

  protected def appConfigReader: ConfigReader[AppConfig]

  protected def createEnv(appName: String, config: Config): Env

  protected def run(config: AppConfig): TaskR[Env, Unit]

  protected def setupMetrics(config: Config): Unit = {
    Kamon.reconfigure(config)
    Kamon.loadReportersFromConfig()
    SystemMetrics.startCollecting()
  }

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replaceAllLiterally("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replaceAllLiterally(".", "/") + "/" + appName
    val config = ConfigFactory.load(
      appConfigName,
      ConfigParseOptions.defaults.setAllowMissing(false),
      ConfigResolveOptions.defaults
    )
    implicit val configReader: ConfigReader[AppConfig] = appConfigReader
    val exitCode = PureconfigLoader.load[AppConfig](config, "app") match {
      case Left(error) =>
        println("There are app configuration errors. Please see details below:")
        println(error)
        1

      case Right(appConfig) =>
        setupMetrics(config)

        val runtime = new zio.Runtime[Env] {
          val Environment: Env = createEnv(appName, config)
          val Platform: Platform = PlatformLive.fromExecutionContext(Environment.dispatcher)
        }

        val app = run(appConfig).map(_ => 0).catchAll { e =>
          ZLogger.error(s"App failed: ${e.getMessage}", e) *> UIO(1)
        }

        runtime.unsafeRun(
          for {
            fiber <- app.fork
            _ <- IO.effectTotal(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
              override def run(): Unit = {
                val _ = runtime.unsafeRunSync(fiber.interrupt)
              }
            }))
            result <- fiber.join
          } yield result
        )
    }

    sys.exit(exitCode)
  }
}
