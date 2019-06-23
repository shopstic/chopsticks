package dev.chopsticks.util.mon

import java.time.Duration

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.MetricReporter
import kamon.metric.MeasurementUnit.Dimension
import kamon.metric.{MetricValue, MetricsSnapshot, PeriodSnapshot, PeriodSnapshotAccumulator}
import squants.information.{Bytes, Gigabytes, Kilobytes, Megabytes}
import squants.time.{Microseconds, Milliseconds, Nanoseconds, Seconds}
import dev.chopsticks.util.implicits.SquantsImplicits._

object KamonConsoleReporter {
  private val BYTES_IN_MEGA = 1024 * 1024
  private val BYTES_IN_GIGA = 1024 * 1024 * 1024
}

final class KamonConsoleReporter extends MetricReporter with StrictLogging {
  import KamonConsoleReporter._

  private val snapshotAccumulator = new PeriodSnapshotAccumulator(Duration.ofDays(365 * 5), Duration.ZERO)

  def start(): Unit = {}

  def stop(): Unit = {}

  private def filterMetrics(metric: MetricValue) = {
    !metric.tags.get("component").contains("system-metrics")
  }

  def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    val _ = snapshotAccumulator.add(
      snapshot.copy(
        metrics = MetricsSnapshot(
          histograms = Seq.empty,
          rangeSamplers = Seq.empty,
          gauges = snapshot.metrics.gauges.filter(filterMetrics),
          counters = snapshot.metrics.counters.filter(filterMetrics)
        )
      )
    )

    val current = snapshotAccumulator.peek()
    val metricValues = current.metrics.counters ++ current.metrics.gauges
    val message = metricValues
      .map { m =>
        val value = m.unit.dimension match {
          case Dimension.Percentage => m.value + "%"

          case Dimension.Time =>
            val v = m.unit.magnitude.scaleFactor match {
              case 1d => Seconds(m.value)
              case 1e-3 => Milliseconds(m.value)
              case 1e-6 => Microseconds(m.value)
              case 1e-9 => Nanoseconds(m.value)
            }
            v.inBestUnit.rounded(2).toString

          case Dimension.Information =>
            val v = m.unit.magnitude.scaleFactor match {
              case 1 => Bytes(m.value)
              case 1024 => Kilobytes(m.value)
              case `BYTES_IN_MEGA` => Megabytes(m.value)
              case `BYTES_IN_GIGA` => Gigabytes(m.value)
            }
            v.inBestUnit.rounded(2).toString

          case _ => m.value
        }
        s"${m.name}=$value"
      }
      .mkString(" ")

    if (message.nonEmpty) {
      logger.info(message)
    }
  }

  def reconfigure(config: Config): Unit = ???
}
