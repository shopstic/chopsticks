package dev.chopsticks.metric.prom

import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricFactory.MetricGroup
import dev.chopsticks.metric._
import dev.chopsticks.metric.prom.PromMetrics._
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}

object TestMetricFactory {
  def apply[C <: MetricGroup](): TestMetricFactory[C] = new TestMetricFactory[C]()
}

final class TestMetricFactory[C <: MetricGroup] extends MetricFactory[C] {
  override def counter(config: CounterConfig[NoLabel] with C): MetricCounter = {
    new PromCounter(
      Counter
        .build(config.name, config.name)
        .create()
    )
  }

  override def counterWithLabels[L <: MetricLabel](
    config: CounterConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricCounter = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    new PromChildCounter(
      Counter
        .build(config.name, config.name)
        .labelNames(names: _*)
        .create()
        .labels(values: _*)
    )
  }

  override def gauge(config: GaugeConfig[NoLabel] with C): MetricGauge = {
    new PromGauge(
      Gauge
        .build(config.name, config.name)
        .create()
    )
  }

  override def gaugeWithLabels[L <: MetricLabel](
    config: GaugeConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricGauge = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    new PromChildGauge(
      Gauge
        .build(config.name, config.name)
        .labelNames(names: _*)
        .create()
        .labels(values: _*)
    )
  }

  override def histogram(config: HistogramConfig[NoLabel] with C): MetricHistogram = {
    new PromHistogram(
      Histogram
        .build(config.name, config.name)
        .buckets(config.buckets: _*)
        .create()
    )
  }

  override def histogramWithLabels[L <: MetricLabel](
    config: HistogramConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricHistogram = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    new PromChildHistogram(
      Histogram
        .build(config.name, config.name)
        .buckets(config.buckets: _*)
        .labelNames(names: _*)
        .create()
        .labels(values: _*)
    )
  }

  override def summary(config: SummaryConfig[NoLabel] with C): MetricSummary = {
    new PromSummary(
      config.quantiles
        .foldLeft(Summary.build(config.name, config.name)) {
          case (s, (quantile, error)) =>
            s.quantile(quantile, error)
        }
        .maxAgeSeconds(config.maxAge.toSeconds)
        .ageBuckets(config.ageBuckets)
        .create()
    )
  }

  override def summaryWithLabels[L <: MetricLabel](
    config: SummaryConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricSummary = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names

    new PromChildSummary(
      config.quantiles
        .foldLeft(Summary.build(config.name, config.name)) {
          case (s, (quantile, error)) =>
            s.quantile(quantile, error)
        }
        .maxAgeSeconds(config.maxAge.toSeconds)
        .ageBuckets(config.ageBuckets)
        .labelNames(names: _*)
        .create()
        .labels(values: _*)
    )
  }
}
