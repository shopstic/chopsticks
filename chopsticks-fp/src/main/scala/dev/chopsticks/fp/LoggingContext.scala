package dev.chopsticks.fp

import dev.chopsticks.fp.log_env.LogCtx

trait LoggingContext {
  implicit val loggingCtx: LogCtx = LogCtx(getClass.getName)
}
