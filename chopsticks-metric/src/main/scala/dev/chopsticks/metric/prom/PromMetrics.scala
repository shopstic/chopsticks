package dev.chopsticks.metric.prom

import dev.chopsticks.metric.MetricReference.MetricReferenceValue
import dev.chopsticks.metric._
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}

import java.util.concurrent.atomic.AtomicReference

object PromMetrics {
  final class PromCounter(counter: Counter) extends MetricCounter {
    override def inc(value: Double): Unit = counter.inc(value)
    override def get: Double = counter.get
  }

  object PromCounter {
    def test: PromCounter = new PromCounter(Counter.build("test", "test").create())
  }

  final class PromChildCounter(counter: Counter.Child) extends MetricCounter {
    override def inc(value: Double): Unit = counter.inc(value)
    override def get: Double = counter.get
  }

  final class PromGauge(gauge: Gauge) extends MetricGauge {
    override def inc(value: Double): Unit = gauge.inc(value)
    override def dec(value: Double): Unit = gauge.dec(value)
    override def set(value: Double): Unit = gauge.set(value)
    override def get: Double = gauge.get
  }

  object PromGauge {
    def test: PromGauge = new PromGauge(Gauge.build("test", "test").create())
  }

  final class PromChildGauge(gauge: Gauge.Child) extends MetricGauge {
    override def inc(value: Double): Unit = gauge.inc(value)
    override def dec(value: Double): Unit = gauge.dec(value)
    override def set(value: Double): Unit = gauge.set(value)
    override def get: Double = gauge.get
  }

  final class PromReference[V: MetricReferenceValue](gauge: Gauge) extends MetricReference[V] {
    private val ref = new AtomicReference(Option.empty[V])
    override def set(value: V): Unit = {
      ref.set(Some(value))
      gauge.set(MetricReferenceValue[V].getValue(value))
    }
    override def get: Option[V] = ref.get()
  }

  object PromReference {
    def test[V: MetricReferenceValue]: PromReference[V] = new PromReference[V](Gauge.build("test", "test").create())
  }
  final class PromHistogram(histogram: Histogram) extends MetricHistogram {
    override def observe(value: Double): Unit = histogram.observe(value)
  }

  final class PromChildReference[V: MetricReferenceValue](gauge: Gauge.Child) extends MetricReference[V] {
    private val ref = new AtomicReference(Option.empty[V])
    override def set(value: V): Unit = {
      ref.set(Some(value))
      gauge.set(MetricReferenceValue[V].getValue(value))
    }
    override def get: Option[V] = ref.get()
  }

  object PromHistogram {
    def test: PromHistogram = {
      new PromHistogram(Histogram.build("test", "test").create())
    }
  }

  final class PromChildHistogram(histogram: Histogram.Child) extends MetricHistogram {
    override def observe(value: Double): Unit = histogram.observe(value)
  }

  final class PromSummary(summary: Summary) extends MetricSummary {
    override def observe(value: Double): Unit = summary.observe(value)
  }

  object PromSummary {
    def test: PromSummary = {
      new PromSummary(Summary.build("test", "test").create())
    }
  }

  final class PromChildSummary(summary: Summary.Child) extends MetricSummary {
    override def observe(value: Double): Unit = summary.observe(value)
  }
}
