package dev.chopsticks.metric

import zio.stream.*

object MetricStreamOps {

  extension [Env, Err, Out](stream: ZStream[Env, Err, Out]) {
    def metricCounter(metric: MetricCounter): ZStream[Env, Err, Out] =
      stream.mapChunks { xs =>
        metric.inc(xs.size)
        xs
      }

    def metricCounter[N](metric: MetricCounter, toNumeric: Out => N)(implicit num: Numeric[N]): ZStream[Env, Err, Out] =
      stream
        .mapChunks { xs =>
          val total = xs.foldLeft(0d)((acc, x) => acc + num.toDouble(toNumeric(x)))
          metric.inc(total)
          xs
        }

    def metricGauge[N](metric: MetricGauge, toValue: Out => N)(implicit num: Numeric[N]): ZStream[Env, Err, Out] =
      stream
        .mapChunks { xs =>
          if (xs.nonEmpty) {
            metric.set(num.toDouble(toValue(xs.last)))
          }
          xs
        }
  }

  extension [Env, Err, In, Out](pipeline: ZPipeline[Env, Err, In, Out]) {
    def metricCounter(metric: MetricCounter): ZPipeline[Env, Err, In, Out] =
      pipeline
        .mapChunks { xs =>
          metric.inc(xs.size)
          xs
        }

    def metricCounter[N](metric: MetricCounter, toNumeric: Out => N)(implicit
      num: Numeric[N]
    ): ZPipeline[Env, Err, In, Out] =
      pipeline
        .mapChunks { xs =>
          val total = xs.foldLeft(0d)((acc, x) => acc + num.toDouble(toNumeric(x)))
          metric.inc(total)
          xs
        }

    def metricGauge[N](metric: MetricGauge, toValue: Out => N)(implicit num: Numeric[N]): ZPipeline[Env, Err, In, Out] =
      pipeline
        .mapChunks { xs =>
          if (xs.nonEmpty) {
            metric.set(num.toDouble(toValue(xs.last)))
          }
          xs
        }
  }
}
