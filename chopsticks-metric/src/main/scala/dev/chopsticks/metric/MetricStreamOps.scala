package dev.chopsticks.metric

import org.apache.pekko.stream.scaladsl.{Flow, Source}

object MetricStreamOps {
  implicit class AkkaStreamSourceMetricOps[+Elem, +Mat](source: Source[Elem, Mat]) {
    def metricCounter(metric: MetricCounter): Source[Elem, Mat] = {
      source
        .map { v =>
          metric.inc()
          v
        }
    }

    def metricCounter[N](metric: MetricCounter, toNumeric: Elem => N)(implicit num: Numeric[N]): Source[Elem, Mat] = {
      source
        .map { v =>
          metric.inc(num.toDouble(toNumeric(v)))
          v
        }
    }

    def metricGauge[N](metric: MetricGauge, toValue: Elem => N)(implicit num: Numeric[N]): Source[Elem, Mat] = {
      source
        .map { v =>
          metric.set(num.toDouble(toValue(v)))
          v
        }
    }
  }

  implicit class AkkaStreamFlowMetricOps[-In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
    def metricCounter(metric: MetricCounter): Flow[In, Out, Mat] = {
      flow
        .map { v =>
          metric.inc()
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
