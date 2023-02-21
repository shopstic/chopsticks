package dev.chopsticks.metric

import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricReference.MetricReferenceValue

trait MetricRegistry[C <: MetricRegistry.MetricGroup] {
  def counter(config: CounterConfig[NoLabel] with C): MetricCounter

  def counterWithLabels[L <: MetricLabel](config: CounterConfig[L] with C, labelValues: LabelValues[L]): MetricCounter

  def gauge(config: GaugeConfig[NoLabel] with C): MetricGauge

  def gaugeWithLabels[L <: MetricLabel](config: GaugeConfig[L] with C, labelValues: LabelValues[L]): MetricGauge

  def reference[V: MetricReferenceValue](config: GaugeConfig[NoLabel] with C): MetricReference[V]

  def referenceWithLabels[L <: MetricLabel, V: MetricReferenceValue](
    config: GaugeConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricReference[V]

  def histogram(config: HistogramConfig[NoLabel] with C): MetricHistogram

  def histogramWithLabels[L <: MetricLabel](
    config: HistogramConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricHistogram

  def summary(config: SummaryConfig[NoLabel] with C): MetricSummary

  def summaryWithLabels[L <: MetricLabel](config: SummaryConfig[L] with C, labelValues: LabelValues[L]): MetricSummary
}

object MetricRegistry {
  trait MetricGroup
}
