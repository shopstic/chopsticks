package dev.chopsticks.fp.iz_logging

object LogCtx {
  implicit def autoLogContext(implicit line: sourcecode.Line, file: sourcecode.FileName): LogCtx = {
    LogCtx(s"${file.value}:${line.value}")
  }
}

final case class LogCtx(sourceLocation: String) extends AnyVal
