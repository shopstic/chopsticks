package dev.chopsticks.metric

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.RLayer

object MetricManagerTest {
  object fooLabel extends MetricLabel
  object barLabel extends MetricLabel

  private val labels = LabelNames of fooLabel and barLabel
  sealed trait FooMetric extends MetricGroup
  object FooTotal extends CounterConfig(labels) with FooMetric

  trait Metrics {
    def fooCounter: MetricCounter
  }

  final case class MetricsConfig(foo: String, bar: Boolean)

  object MetricsFactory extends MetricServiceFactory[FooMetric, MetricsConfig, Metrics] {
    override def create(registry: MetricRegistry[FooMetric], config: MetricsConfig): Metrics = {
      val labels = LabelValues.of(fooLabel -> config.foo).and(barLabel -> config.bar.toString)
      new Metrics {
        override val fooCounter: MetricCounter = registry.counterWithLabels(FooTotal, labels)
      }
    }
  }

  def liveManager: RLayer[MetricRegistryFactory[FooMetric], MetricServiceManager[MetricsConfig, Metrics]] =
    MetricServiceManager.live(MetricsFactory)
}
