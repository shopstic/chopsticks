package dev.chopsticks.metric

import zio.Has

package object log {
  type MetricLogger = Has[MetricLogger.Service]
}
