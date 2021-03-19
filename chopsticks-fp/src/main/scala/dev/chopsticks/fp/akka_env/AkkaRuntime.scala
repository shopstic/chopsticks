package dev.chopsticks.fp.akka_env

import akka.actor.CoordinatedShutdown
import akka.actor.CoordinatedShutdown.JvmExitReason
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import zio.internal.tracing.TracingConfig
import zio.{Cause, Runtime, URIO, URLayer, ZIO}

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

object AkkaRuntime {
  trait Service[R] {
    def runtime: Runtime[R]
  }

  def get[R: zio.Tag]: URIO[AkkaRuntime[R], Runtime[R]] = ZIO.access[AkkaRuntime[R]](_.get.runtime)

  def live[R: zio.Tag]: URLayer[R with IzLogging with AkkaEnv with HoconConfig, AkkaRuntime[R]] = {
    val effect = for {
      hoconConfig <- HoconConfig.get
      actorSystem <- AkkaEnv.actorSystem
      dispatcher <- AkkaEnv.dispatcher
      logger <- IzLogging.logger
      env <- ZIO.environment[R]
    } yield {
      val zioTracingEnabled = Try(hoconConfig.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)

      new Service[R] {
        override val runtime: Runtime[R] = {
          val shutdown: CoordinatedShutdown = CoordinatedShutdown(actorSystem)
          val platform: zio.internal.Platform = new zio.internal.Platform.Proxy(
            zio.internal.Platform
              .fromExecutionContext(dispatcher)
              .withTracingConfig(if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled)
          ) {
            private val isShuttingDown = new AtomicBoolean(false)

            override def reportFailure(cause: Cause[Any]): Unit = {
              if (cause.died && shutdown.shutdownReason().isEmpty && isShuttingDown.compareAndSet(false, true)) {
                logger.error(s"Application failure:\n${cause.prettyPrint -> "cause" -> null}")
                val _ = shutdown.run(JvmExitReason)
              }
            }
          }

          Runtime(env, platform)
        }
      }
    }

    effect.toLayer
  }
}
