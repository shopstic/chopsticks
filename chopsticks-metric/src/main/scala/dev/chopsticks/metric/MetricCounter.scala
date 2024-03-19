package dev.chopsticks.metric

import zio.ZIO

trait MetricCounter {
  def inc(value: Double): Unit
  def get: Double
  def inc(): Unit = inc(1.0d)
  def inc(value: Long): Unit = inc(value.toDouble)
  def inc(value: Int): Unit = inc(value.toDouble)
  def timeM[R, E, A](f: ZIO[R, E, A]): ZIO[R, E, A] = {
    f.timed.map {
      case (d, v) =>
        inc(d.toNanos)
        v
    }
  }
}
