package dev.chopsticks.fp

import dev.chopsticks.fp.iz_logging.LogCtx

trait LoggingContext {
  implicit val loggingCtx: LogCtx = LogCtx(getClass.getName)
}
