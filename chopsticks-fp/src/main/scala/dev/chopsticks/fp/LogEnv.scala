package dev.chopsticks.fp

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

final case class LogCtx(name: String) extends AnyVal

trait LogEnv {
  def logger(implicit ctx: LogCtx): Logger
}

object LogEnv {
  trait Live extends LogEnv {
    def logger(implicit ctx: LogCtx): Logger = Logger(LoggerFactory.getLogger(ctx.name))
  }
}
