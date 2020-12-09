package dev.chopsticks.metric

import java.time.{Instant, LocalDate}

object MetricReference {
  trait MetricReferenceValue[V] {
    def getValue(reference: V): Double
  }

  object MetricReferenceValue {
    def apply[V: MetricReferenceValue]: MetricReferenceValue[V] = implicitly[MetricReferenceValue[V]]

    implicit val instantMetricValue: MetricReferenceValue[Instant] = _.toEpochMilli.toDouble
    implicit val localDateMetricValue: MetricReferenceValue[LocalDate] = _.toEpochDay.toDouble
  }
}

trait MetricReference[V] {
  def set(value: V): Unit
  def get: Option[V]
}
