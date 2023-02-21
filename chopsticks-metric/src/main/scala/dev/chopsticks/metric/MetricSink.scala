package dev.chopsticks.metric

import zio.stream.ZSink
import zio.stream.Sink
import zio.ZIO

object MetricSink {
  def counter[V](metric: MetricCounter): Sink[Nothing, V, Nothing, Unit] =
    ZSink.foreachChunk[Any, Nothing, V](xs => ZIO.succeed(metric.inc(xs.length)))

  def counter[V, N](metric: MetricCounter, toNumeric: V => N)(implicit
    num: Numeric[N]
  ): Sink[Nothing, V, Nothing, Unit] =
    ZSink.foreachChunk[Any, Nothing, V] { xs =>
      ZIO.succeed {
        val total = xs.foldLeft(0d)((acc, x) => acc + num.toDouble(toNumeric(x)))
        metric.inc(total)
      }
    }

  def gauge[V, N](metric: MetricGauge, toNumeric: V => N)(implicit num: Numeric[N]): Sink[Nothing, V, Nothing, Unit] =
    ZSink.foreachChunk[Any, Nothing, V] { xs =>
      ZIO
        .succeed(metric.set(num.toDouble(toNumeric(xs.last)))).when(xs.nonEmpty)
        .when(xs.nonEmpty)
    }
}
