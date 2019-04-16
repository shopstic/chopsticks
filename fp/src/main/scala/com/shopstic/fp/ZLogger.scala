package com.shopstic.fp

import scalaz.zio.ZIO

object ZLogger {
  def debug(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.logger.debug(message))
  }

  def info(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.logger.info(message))
  }

  def warn(message: String)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.logger.warn(message))
  }

  def error(message: String, cause: Throwable)(implicit ctx: LogCtx): ZIO[LogEnv, Nothing, Unit] = {
    ZIO.access[LogEnv](_.logger.error(message, cause))
  }
}
