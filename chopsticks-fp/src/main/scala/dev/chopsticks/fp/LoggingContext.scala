package dev.chopsticks.fp

trait LoggingContext {
  implicit val loggingCtx: LogCtx = LogCtx(getClass.getName)
}
