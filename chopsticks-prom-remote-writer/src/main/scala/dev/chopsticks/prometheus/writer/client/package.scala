package dev.chopsticks.prometheus.writer

import zio.Has

package object client {
  type PrometheusRemoteWriterClient = Has[PrometheusRemoteWriterClient.Service]
}
