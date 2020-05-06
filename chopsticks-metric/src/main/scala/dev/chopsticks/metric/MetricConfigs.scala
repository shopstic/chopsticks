package dev.chopsticks.metric

import pureconfig.{PascalCase, SnakeCase}

import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

object MetricConfigs {

  private def toMetricName(className: String): String = {
    SnakeCase.fromTokens(PascalCase.toTokens(className.replaceFirst("\\$$", "")))
  }

  abstract class MetricLabel {
    lazy val name: String = toMetricName(this.getClass.getSimpleName)
  }

  class NoLabel extends MetricLabel

  object NoLabel extends NoLabel

  object LabelNames {
    def apply[L <: MetricLabel](label: L): LabelNames[L] = {
      new LabelNames[L](Vector(label.name))
    }
    def of[L <: MetricLabel](label: L): LabelNames[L] = apply(label)
    val none = new LabelNames[NoLabel](Vector.empty)
  }

  final class LabelNames[+M <: MetricLabel] private (val names: Vector[String]) {
    def and[L <: MetricLabel](label: L): LabelNames[M with L] = {
      new LabelNames[M with L](names :+ label.name)
    }
  }

  object LabelValues {
    def apply[L <: MetricLabel](mapping: (L, String)): LabelValues[L] = {
      new LabelValues[L](Map(mapping._1.name -> mapping._2))
    }
    def of[L <: MetricLabel](mapping: (L, String)): LabelValues[L] = apply(mapping)
  }

  final class LabelValues[+M <: MetricLabel] private (val map: Map[String, String]) {
    def and[L <: MetricLabel](mapping: (L, String)): LabelValues[M with L] = {
      new LabelValues[M with L](map.updated(mapping._1.name, mapping._2))
    }
  }

  abstract class MetricConfig[L <: MetricLabel](val labelNames: LabelNames[L]) {
    lazy val name: String = toMetricName(this.getClass.getSimpleName)
  }

  abstract class CounterConfig[L <: MetricLabel](override val labelNames: LabelNames[L])
      extends MetricConfig[L](labelNames)
  abstract class NoLabelCounterConfig extends MetricConfig[NoLabel](LabelNames.none)

  abstract class GaugeConfig[L <: MetricLabel](override val labelNames: LabelNames[L])
      extends MetricConfig[L](labelNames)
  abstract class NoLabelGaugeConfig extends MetricConfig[NoLabel](LabelNames.none)

  abstract class HistogramConfig[L <: MetricLabel](override val labelNames: LabelNames[L], val buckets: List[Double])
      extends MetricConfig[L](labelNames)
  abstract class NoLabelHistogramConfig(val buckets: List[Double]) extends MetricConfig[NoLabel](LabelNames.none)

  abstract class SummaryConfig[L <: MetricLabel](
    override val labelNames: LabelNames[L],
    val quantiles: ListMap[Double, Double],
    val maxAge: FiniteDuration,
    val ageBuckets: Int
  ) extends MetricConfig[L](labelNames)
  abstract class NoLabelSummaryConfig(
    val quantiles: ListMap[Double, Double],
    val maxAge: FiniteDuration,
    val ageBuckets: Int
  ) extends MetricConfig[NoLabel](LabelNames.none)
}
