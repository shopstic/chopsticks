package dev.chopsticks.metric.prom

import dev.chopsticks.metric.MetricConfigs.*
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.*
import dev.chopsticks.metric.prom.PromMetrics.*
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}
import zio.{ULayer, ZLayer}

object TestMetricRegistry {
  def apply[C <: MetricGroup](): TestMetricRegistry[C] = new TestMetricRegistry[C]()

  def live[C <: MetricGroup: zio.Tag]: ULayer[MetricRegistry[C]] = {
    ZLayer.succeed(new TestMetricRegistry[C])
  }
}

final class TestMetricRegistry[C <: MetricGroup] extends MetricRegistry[C] {
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

  override def reference[V: MetricReference.MetricReferenceValue](config: GaugeConfig[NoLabel] with C)
    : MetricReference[V] = {
    new PromReference[V](
      Gauge
        .build(config.name, config.name)
        .create()
    )
  }

  override def referenceWithLabels[L <: MetricLabel, V: MetricReference.MetricReferenceValue](
    config: GaugeConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricReference[V] = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    new PromChildReference[V](
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
