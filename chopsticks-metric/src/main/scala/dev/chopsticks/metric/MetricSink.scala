package dev.chopsticks.metric

import akka.Done
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

object MetricSink {
  def counter[V](metric: MetricCounter): Sink[V, Future[Done]] = {
    Sink.foreach[V](_ => metric.inc())
  }

  def counter[V, N](metric: MetricCounter, toNumeric: V => N)(implicit num: Numeric[N]): Sink[V, Future[Done]] = {
    Sink.foreach[V](v => metric.inc(num.toDouble(toNumeric(v))))
  }

  def gauge[V, N](metric: MetricGauge, toNumeric: V => N)(implicit num: Numeric[N]): Sink[V, Future[Done]] = {
    Sink.foreach[V](v => metric.set(num.toDouble(toNumeric(v))))
  }
}
