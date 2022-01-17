package dev.chopsticks.fp.iz_logging

import izumi.logstage.api.Log
import izumi.logstage.api.Log.LogArg
import izumi.logstage.api.rendering.AnyEncoded

import scala.collection.immutable.Queue

trait EncodingAwareLogCtx[-E <: AnyEncoded] {
  type Self <: EncodingAwareLogCtx[E]

  protected def withLogArgs(logArgs: Seq[LogArg]): Self
  def withAttributes(attrs: (String, E)*): Self = {
    withLogArgs(attrs.map { case (k, v) =>
      LogArg(Seq(k), v.value, hiddenName = false, v.codec)
    })
  }
}

object LogCtx {
  implicit def auto(implicit line: sourcecode.Line, file: sourcecode.FileName): LogCtx = {
    LogCtx(s"${file.value}:${line.value}", Log.Level.Info)
  }
}

final case class LogCtx(sourceLocation: String, level: Log.Level, logArgs: Queue[LogArg] = Queue.empty)
    extends EncodingAwareLogCtx[AnyEncoded] {
  override type Self = LogCtx
  override protected def withLogArgs(newLogArgs: Seq[LogArg]): LogCtx = {
    copy(logArgs = logArgs ++ newLogArgs)
  }
}
