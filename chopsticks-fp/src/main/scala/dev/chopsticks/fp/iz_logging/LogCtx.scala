package dev.chopsticks.fp.iz_logging

import izumi.logstage.api.Log

object LogCtx {
  implicit def autoLogContext(implicit line: sourcecode.Line, file: sourcecode.FileName): LogCtx = {
    LogCtx(s"${file.value}:${line.value}", Log.Level.Info)
  }
}

final case class LogCtx(sourceLocation: String, level: Log.Level)
