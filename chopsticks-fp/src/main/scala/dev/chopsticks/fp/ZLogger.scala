package dev.chopsticks.fp

import dev.chopsticks.fp.log_env.{LogCtx, LogEnv}
import zio.ZIO

object ZLogger {
  def debug(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.get.logger.debug(message))
  }

  def info(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.get.logger.info(message))
  }

  def warn(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.get.logger.warn(message))
  }

  def error(message: String, cause: Throwable)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.get.logger.error(message, cause))
  }
}
