package dev.chopsticks.metric.prom

import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricService.MetricGroup
import dev.chopsticks.metric._
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}

import scala.collection.mutable

object PromMetricService {
  final class PromCounter(counter: Counter) extends MetricCounter {
    override def inc(value: Double): Unit = counter.inc(value)
    override def get(): Double = counter.get()
  }

  final class PromChildCounter(counter: Counter.Child) extends MetricCounter {
    override def inc(value: Double): Unit = counter.inc(value)
    override def get(): Double = counter.get()
  }

  final class PromGauge(gauge: Gauge) extends MetricGauge {
    override def inc(value: Double): Unit = gauge.inc(value)
    override def dec(value: Double): Unit = gauge.dec(value)
    override def set(value: Double): Unit = gauge.set(value)
    override def get(): Double = gauge.get()
  }

  final class PromChildGauge(gauge: Gauge.Child) extends MetricGauge {
    override def inc(value: Double): Unit = gauge.inc(value)
    override def dec(value: Double): Unit = gauge.dec(value)
    override def set(value: Double): Unit = gauge.set(value)
    override def get(): Double = gauge.get()
  }

  final class PromHistogram(histogram: Histogram) extends MetricHistogram {
    override def observe(value: Double): Unit = histogram.observe(value)
  }

  final class PromChildHistogram(histogram: Histogram.Child) extends MetricHistogram {
    override def observe(value: Double): Unit = histogram.observe(value)
  }

  final class PromSummary(summary: Summary) extends MetricSummary {
    override def observe(value: Double): Unit = summary.observe(value)
  }

  final class PromChildSummary(summary: Summary.Child) extends MetricSummary {
    override def observe(value: Double): Unit = summary.observe(value)
  }

  private val counters = mutable.Map.empty[String, Counter]
  private val gauges = mutable.Map.empty[String, Gauge]
  private val histograms = mutable.Map.empty[String, Histogram]
  private val summaries = mutable.Map.empty[String, Summary]

  def prefixMetric(config: MetricConfig[_], prefix: String): String = {
    if (prefix.nonEmpty) prefix + "_" + config.name else config.name
  }

  def apply[C <: MetricGroup](prefix: String): PromMetricService[C] = {
    new PromMetricService[C](prefix)
  }
}

final class PromMetricService[C <: MetricGroup](prefix: String) extends MetricService[C] {
  import PromMetricService._

  override def counter(config: CounterConfig[NoLabel] with C): MetricCounter = {
    val prefixedName = prefixMetric(config, prefix)

    val promCounter = counters.synchronized {
      counters.getOrElseUpdate(prefixedName, {
        Counter
          .build(prefixedName, prefixedName)
          .register()
      })
    }

    new PromCounter(promCounter)
  }

  override def counterWithLabels[L <: MetricLabel](
    config: CounterConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricCounter = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    val prefixedName = prefixMetric(config, prefix)

    val promCounter = counters.synchronized {
      counters.getOrElseUpdate(prefixedName, {
        Counter
          .build(prefixedName, prefixedName)
          .labelNames(names: _*)
          .register()
      })
    }

    new PromChildCounter(promCounter.labels(values: _*))
  }

  override def gauge(config: GaugeConfig[NoLabel] with C): MetricGauge = {
    val prefixedName = prefixMetric(config, prefix)

    val promGauge = gauges.synchronized {
      gauges.getOrElseUpdate(prefixedName, {
        Gauge
          .build(prefixedName, prefixedName)
          .register()
      })
    }

    new PromGauge(promGauge)
  }

  override def gaugeWithLabels[L <: MetricLabel](
    config: GaugeConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricGauge = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    val prefixedName = prefixMetric(config, prefix)

    val promGauge = gauges.synchronized {
      gauges.getOrElseUpdate(prefixedName, {
        Gauge
          .build(prefixedName, prefixedName)
          .labelNames(names: _*)
          .register()
      })
    }

    new PromChildGauge(promGauge.labels(values: _*))
  }

  override def histogram(config: HistogramConfig[NoLabel] with C): MetricHistogram = {
    val prefixedName = prefixMetric(config, prefix)

    val promHistogram = histograms.synchronized {
      histograms.getOrElseUpdate(prefixedName, {
        Histogram
          .build(prefixedName, prefixedName)
          .buckets(config.buckets: _*)
          .register()
      })
    }

    new PromHistogram(promHistogram)
  }

  override def histogramWithLabels[L <: MetricLabel](
    config: HistogramConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricHistogram = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    val prefixedName = prefixMetric(config, prefix)

    val promHistogram = histograms.synchronized {
      histograms.getOrElseUpdate(prefixedName, {
        Histogram
          .build(prefixedName, prefixedName)
          .buckets(config.buckets: _*)
          .labelNames(names: _*)
          .register()
      })
    }

    new PromChildHistogram(promHistogram.labels(values: _*))
  }

  override def summary(config: SummaryConfig[NoLabel] with C): MetricSummary = {
    val prefixedName = prefixMetric(config, prefix)

    val promSummary = summaries.synchronized {
      summaries.getOrElseUpdate(
        prefixedName, {
          config.quantiles
            .foldLeft(Summary.build(prefixedName, prefixedName)) {
              case (s, (quantile, error)) =>
                s.quantile(quantile, error)
            }
            .maxAgeSeconds(config.maxAge.toSeconds)
            .ageBuckets(config.ageBuckets)
            .register()
        }
      )
    }

    new PromSummary(promSummary)
  }

  override def summaryWithLabels[L <: MetricLabel](
    config: SummaryConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricSummary = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    val prefixedName = prefixMetric(config, prefix)

    val promSummary = summaries.synchronized {
      summaries.getOrElseUpdate(
        prefixedName, {
          config.quantiles
            .foldLeft(Summary.build(prefixedName, prefixedName)) {
              case (s, (quantile, error)) =>
                s.quantile(quantile, error)
            }
            .maxAgeSeconds(config.maxAge.toSeconds)
            .ageBuckets(config.ageBuckets)
            .labelNames(names: _*)
            .register()
        }
      )
    }

    new PromChildSummary(promSummary.labels(values: _*))
  }
}
