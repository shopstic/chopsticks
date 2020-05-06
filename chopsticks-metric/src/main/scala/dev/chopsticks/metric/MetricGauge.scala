package dev.chopsticks.metric

import zio.ZIO
import zio.clock.Clock

trait MetricGauge {
  def inc(value: Double): Unit
  def dec(value: Double): Unit
  def set(value: Double): Unit
  def get(): Double

  def inc(): Unit = inc(1.0d)
  def inc(value: Long): Unit = inc(value.toDouble)
  def inc(value: Int): Unit = inc(value.toDouble)

  def dec(): Unit = dec(1.0d)
  def dec(value: Long): Unit = dec(value.toDouble)
  def dec(value: Int): Unit = dec(value.toDouble)

  def set(value: Long): Unit = set(value.toDouble)
  def set(value: Int): Unit = set(value.toDouble)

  def timeM[R, E, A](f: ZIO[R, E, A]): ZIO[R with Clock, E, A] = {
    f.timed.map {
      case (d, v) =>
        set(d.toNanos)
        v
    }
  }
}
