//package dev.chopsticks.fp.pekko_env
//
//import org.apache.pekko.actor.CoordinatedShutdown
//import org.apache.pekko.actor.CoordinatedShutdown.JvmExitReason
//import dev.chopsticks.fp.config.HoconConfig
//import dev.chopsticks.fp.iz_logging.IzLogging
//import zio.{Cause, Runtime, URIO, URLayer, ZIO, ZLayer}
//
//import java.util.concurrent.atomic.AtomicBoolean
//import scala.util.Try
//
////trait PekkoRuntime[R] {
////  def runtime: Runtime[R]
////}
////
////object PekkoRuntime {
////  def get[R: zio.Tag]: URIO[PekkoRuntime[R], Runtime[R]] = ZIO.service[PekkoRuntime[R]].map(_.runtime)
////
////  def live[R: zio.Tag]: URLayer[R with IzLogging with PekkoEnv with HoconConfig, PekkoRuntime[R]] = {
////    ZLayer.scoped {
////      for {
////        hoconConfig <- HoconConfig.get
////        actorSystem <- PekkoEnv.actorSystem
////        dispatcher <- PekkoEnv.dispatcher
////        logger <- IzLogging.logger
////        env <- ZIO.environment[R]
////      } yield {
////        val zioTracingEnabled = Try(hoconConfig.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
////
////        // todo fixme
////        new PekkoRuntime[R] {
////          override val runtime: Runtime[R] = {
////            val shutdown: CoordinatedShutdown = CoordinatedShutdown(actorSystem)
////
//////            val platform = zio.internal.Platform
//////              .fromExecutionContext(dispatcher)
//////              .withTracingConfig(if (zioTracingEnabled) Tracing.enabled else Tracing.disabled)
//////
//////            val platform: zio.internal.Platform = new zio.internal.Platform.Proxy(
//////              zio.internal.Platform
//////                .fromExecutionContext(dispatcher)
//////                .withTracingConfig(if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled)
//////            ) {
//////              private val isShuttingDown = new AtomicBoolean(false)
//////
//////              override def reportFailure(cause: Cause[Any]): Unit = {
//////                if (cause.died && shutdown.shutdownReason().isEmpty && isShuttingDown.compareAndSet(false, true)) {
//////                  logger.error(s"Application failure:\n${cause.prettyPrint -> "cause" -> null}")
//////                  val _ = shutdown.run(JvmExitReason)
//////                }
//////              }
//////            }
//////
//////            val isShuttingDown = new AtomicBoolean(false)
//////
//////            // Customize the platform further as needed, especially for error handling.
//////            val customPlatform = platform.withReportFailure { cause =>
//////              if (cause.died && shutdown.shutdownReason().isEmpty && isShuttingDown.compareAndSet(false, true)) {
//////                logger.error(s"Application failure:\n${cause.prettyPrint}")
//////                val _ = shutdown.run(JvmExitReason)
//////              }
//////            }
////
////            zio.Runtime.default
////          }
////        }
////      }
////    }
////  }
////}
