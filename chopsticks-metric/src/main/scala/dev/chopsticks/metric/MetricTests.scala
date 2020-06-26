package dev.chopsticks.metric

import java.io.CharArrayWriter

import dev.chopsticks.metric.MetricConfigs.{
  CounterConfig,
  GaugeConfig,
  HistogramConfig,
  LabelNames,
  LabelValues,
  MetricLabel,
  NoLabelCounterConfig
}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.prom.PromMetricRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import zio.{Has, URLayer, ZIO, ZLayer}

object MetricTests {
  object WritesPerTx extends MetricLabel
  object TxParallelism extends MetricLabel

  object Foo {
    sealed trait Metric extends MetricGroup
    object SequentialWritesTotal extends CounterConfig(LabelNames of WritesPerTx) with Metric
    def createCounter(factory: MetricRegistry.Service[Metric], labels: LabelValues[WritesPerTx.type]): MetricCounter = {
      factory.counterWithLabels(
        Foo.SequentialWritesTotal,
        labels
      )
    }
  }

  object Bar {
    object Partition extends MetricLabel
    sealed trait Metric extends MetricGroup
    object RandomWritesTotal extends GaugeConfig(LabelNames of WritesPerTx and TxParallelism) with Metric
    object LatencyDistribution extends HistogramConfig(LabelNames of Partition, List(0.01, 0.5, 0.9)) with Metric
  }

  object Baz {
    sealed trait Metric extends MetricGroup
    object NoLabelTest extends NoLabelCounterConfig with Metric
  }

  def app
    : URLayer[MetricRegistry[Baz.Metric] with MetricRegistry[Bar.Metric] with MetricRegistry[Foo.Metric], Has[Unit]] = {
    val effect = for {
      fooMetrics <- ZIO.access[MetricRegistry[Foo.Metric]](_.get)
      barMetrics <- ZIO.access[MetricRegistry[Bar.Metric]](_.get)
      bazMetrics <- ZIO.access[MetricRegistry[Baz.Metric]](_.get)
    } yield {
      val labelValues = LabelValues of WritesPerTx -> "123" and TxParallelism -> "456"

      val counter = Foo.createCounter(fooMetrics, labelValues)

      val counter2 = fooMetrics.counterWithLabels(
        Foo.SequentialWritesTotal,
        labelValues
      )

      val counter4 = bazMetrics.counter(Baz.NoLabelTest)
      counter4.inc(123)

      val gauge = barMetrics.gaugeWithLabels(
        Bar.RandomWritesTotal,
        labelValues
      )

      val hist1 = barMetrics.histogramWithLabels(Bar.LatencyDistribution, LabelValues of Bar.Partition -> "1")
      val hist2 = barMetrics.histogramWithLabels(Bar.LatencyDistribution, LabelValues of Bar.Partition -> "1")
      val hist3 = barMetrics.histogramWithLabels(Bar.LatencyDistribution, LabelValues of Bar.Partition -> "2")

      hist1.observe(0)
      hist1.observe(0.1)
      hist1.observe(0.2)
      hist2.observe(0.9)

      hist3.observe(0.8)
      hist3.observe(0.85)
      hist3.observe(0.91)

      counter.inc(123)
      gauge.set(999)
      println(counter2.get)
      println(gauge.get)

      val writer = new CharArrayWriter()
      TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples())
      println(writer.toString)
    }

    ZLayer.fromEffect(effect)
  }

  def main(args: Array[String]): Unit = {
    val _ = zio.Runtime.default.unsafeRunSync(
      app
        .build
        .provideLayer(
          PromMetricRegistry.live[Foo.Metric]("foo") ++ PromMetricRegistry.live[Bar.Metric](
            "bar"
          ) ++ PromMetricRegistry.live[Baz.Metric]("baz")
        )
        .useNow
    )

    val writer = new CharArrayWriter()
    TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples())
    assert(writer.toString.linesIterator.forall(_.startsWith("#")))
  }
}
