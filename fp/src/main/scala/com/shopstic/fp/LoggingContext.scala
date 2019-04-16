package com.shopstic.fp

trait LoggingContext {
  implicit val loggingCtx: LogCtx = LogCtx(getClass.getName)
}
