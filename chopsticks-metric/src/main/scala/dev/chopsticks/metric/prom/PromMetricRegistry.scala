package dev.chopsticks.metric.prom

import java.util.concurrent.ConcurrentLinkedQueue
import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricReference.MetricReferenceValue
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import dev.chopsticks.metric.prom.PromMetrics._
import io.prometheus.client._
import zio.{ULayer, URLayer, ZIO, ZLayer}

import scala.collection.mutable

object PromMetricRegistry {
  private val counters = mutable.Map.empty[String, Counter]
  private val gauges = mutable.Map.empty[String, Gauge]
  private val histograms = mutable.Map.empty[String, Histogram]
  private val summaries = mutable.Map.empty[String, Summary]

  def prefixMetric(config: MetricConfig[_], prefix: String): String = {
    if (prefix.nonEmpty) prefix + "_" + config.name else config.name
  }

  def live[C <: MetricGroup: zio.Tag](
    prefix: String,
    registry: CollectorRegistry
  ): ULayer[MetricRegistry[C]] = {
    ZLayer.succeed(registry) >>> live[C](prefix)
  }

  def live[C <: MetricGroup: zio.Tag](prefix: String): URLayer[CollectorRegistry, MetricRegistry[C]] = {
    val managed = for {
      collector <- ZIO.service[CollectorRegistry]
      registry <- ZIO.acquireRelease {
        ZIO.succeed(new PromMetricRegistry[C](prefix, collector))
      } { registry =>
        ZIO.succeed(registry.removeAll())
      }
    } yield registry

    ZLayer.scoped(managed)
  }
}

final class PromMetricRegistry[C <: MetricGroup](
  prefix: String,
  registry: CollectorRegistry
) extends MetricRegistry[C] {
  import PromMetricRegistry._

  private val cleanUpQueue = new ConcurrentLinkedQueue[() => Unit]()

  private[prom] def removeAll(): Unit = {
    import scala.jdk.CollectionConverters._
    cleanUpQueue.iterator().asScala.foreach(_())
  }

  private def removeMetric[SC <: SimpleCollector[_]](
    map: mutable.Map[String, SC],
    prefixedName: String
  ): Unit = {
    map.synchronized {
      map.get(prefixedName).foreach { metric =>
        val _ = map.remove(prefixedName)
        registry.unregister(metric)
      }
    }
  }

  private def removeMetricWithLabels[SC <: SimpleCollector[_], L <: MetricLabel](
    map: mutable.Map[String, SC],
    prefixedName: String,
    values: Seq[String]
  ): Unit = {
    map.synchronized {
      map.get(prefixedName).foreach { metric =>
        metric.remove(values: _*)
      }
    }
  }

  override def counter(config: CounterConfig[NoLabel] with C): MetricCounter = {
    val prefixedName = prefixMetric(config, prefix)

    val promCounter = counters.synchronized {
      counters.getOrElseUpdate(
        prefixedName, {
          val metric = Counter.build(prefixedName, prefixedName).create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetric(counters, prefixedName))
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
      counters.getOrElseUpdate(
        prefixedName, {
          val metric = Counter
            .build(prefixedName, prefixedName)
            .labelNames(names: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }
    val _ = cleanUpQueue.add(() => removeMetricWithLabels(counters, prefixedName, values))
    new PromChildCounter(promCounter.labels(values: _*))
  }

  override def gauge(config: GaugeConfig[NoLabel] with C): MetricGauge = {
    val prefixedName = prefixMetric(config, prefix)

    val promGauge = gauges.synchronized {
      gauges.getOrElseUpdate(
        prefixedName, {
          val metric = Gauge
            .build(prefixedName, prefixedName)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetric(gauges, prefixedName))
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
      gauges.getOrElseUpdate(
        prefixedName, {
          val metric = Gauge
            .build(prefixedName, prefixedName)
            .labelNames(names: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetricWithLabels(gauges, prefixedName, values))
    new PromChildGauge(promGauge.labels(values: _*))
  }

  override def reference[V: MetricReferenceValue](config: GaugeConfig[NoLabel] with C): MetricReference[V] = {
    val prefixedName = prefixMetric(config, prefix)

    val promGauge = gauges.synchronized {
      gauges.getOrElseUpdate(
        prefixedName, {
          val metric = Gauge
            .build(prefixedName, prefixedName)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetric(gauges, prefixedName))
    new PromReference[V](promGauge)
  }

  override def referenceWithLabels[L <: MetricLabel, V: MetricReferenceValue](
    config: GaugeConfig[L] with C,
    labelValues: LabelValues[L]
  ): MetricReference[V] = {
    val values = config.labelNames.names.map(k => labelValues.map(k))
    val names = config.labelNames.names
    val prefixedName = prefixMetric(config, prefix)

    val promGauge = gauges.synchronized {
      gauges.getOrElseUpdate(
        prefixedName, {
          val metric = Gauge
            .build(prefixedName, prefixedName)
            .labelNames(names: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetricWithLabels(gauges, prefixedName, values))
    new PromChildReference[V](promGauge.labels(values: _*))
  }

  override def histogram(config: HistogramConfig[NoLabel] with C): MetricHistogram = {
    val prefixedName = prefixMetric(config, prefix)

    val promHistogram = histograms.synchronized {
      histograms.getOrElseUpdate(
        prefixedName, {
          val metric = Histogram
            .build(prefixedName, prefixedName)
            .buckets(config.buckets: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetric(histograms, prefixedName))
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
      histograms.getOrElseUpdate(
        prefixedName, {
          val metric = Histogram
            .build(prefixedName, prefixedName)
            .buckets(config.buckets: _*)
            .labelNames(names: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetricWithLabels(histograms, prefixedName, values))
    new PromChildHistogram(promHistogram.labels(values: _*))
  }

  override def summary(config: SummaryConfig[NoLabel] with C): MetricSummary = {
    val prefixedName = prefixMetric(config, prefix)

    val promSummary = summaries.synchronized {
      summaries.getOrElseUpdate(
        prefixedName, {
          val metric = config.quantiles
            .foldLeft(Summary.build(prefixedName, prefixedName)) {
              case (s, (quantile, error)) =>
                s.quantile(quantile, error)
            }
            .maxAgeSeconds(config.maxAge.toSeconds)
            .ageBuckets(config.ageBuckets)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetric(summaries, prefixedName))
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
          val metric = config.quantiles
            .foldLeft(Summary.build(prefixedName, prefixedName)) {
              case (s, (quantile, error)) =>
                s.quantile(quantile, error)
            }
            .maxAgeSeconds(config.maxAge.toSeconds)
            .ageBuckets(config.ageBuckets)
            .labelNames(names: _*)
            .create()
          registry.register(metric)
          metric
        }
      )
    }

    val _ = cleanUpQueue.add(() => removeMetricWithLabels(summaries, prefixedName, values))
    new PromChildSummary(promSummary.labels(values: _*))
  }
}
