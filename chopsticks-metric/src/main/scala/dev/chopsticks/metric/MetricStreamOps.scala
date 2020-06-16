package dev.chopsticks.metric

import akka.stream.scaladsl.Flow

object MetricStreamOps {
  implicit class AkkaStreamFlowMetricOps[-In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
    def metricCounter(metric: MetricCounter): Flow[In, Out, Mat] = {
      flow
        .map { v =>
          metric.inc(1.0)
          v
        }
    }

    def metricCounter[N](metric: MetricCounter, toNumeric: Out => N)(implicit num: Numeric[N]): Flow[In, Out, Mat] = {
      flow
        .map { v =>
          metric.inc(num.toDouble(toNumeric(v)))
          v
        }
    }

    def metricGauge[N](metric: MetricGauge, toValue: Out => N)(implicit num: Numeric[N]): Flow[In, Out, Mat] = {
      flow
        .map { v =>
          metric.set(num.toDouble(toValue(v)))
          v
        }
    }
  }
}
