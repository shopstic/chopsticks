package dev.chopsticks.fp.iz_logging

import izumi.logstage.api.Log
import izumi.logstage.api.logger.{LogRouter, LogSink}
import izumi.logstage.api.routing.ConfigurableLogRouter
import zio.{ULayer, ZLayer}

object IzLoggingRouter {
  trait Service {
    def create(threshold: Log.Level, sinks: Seq[LogSink]): LogRouter
  }

  def live: ULayer[IzLoggingRouter] = {
    ZLayer.succeed((threshold: Log.Level, sinks: Seq[LogSink]) => {
      ConfigurableLogRouter(threshold, sinks)
    })
  }
}
