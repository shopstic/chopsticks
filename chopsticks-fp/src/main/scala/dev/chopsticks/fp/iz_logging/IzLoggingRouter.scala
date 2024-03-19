package dev.chopsticks.fp.iz_logging

import izumi.logstage.api.Log
import izumi.logstage.api.logger.{LogQueue, LogRouter, LogSink}
import izumi.logstage.api.routing.ConfigurableLogRouter
import zio.{ULayer, ZLayer}

trait IzLoggingRouter {
  def create(threshold: Log.Level, sinks: Seq[LogSink], buffer: LogQueue): LogRouter
}

object IzLoggingRouter {
  def live: ULayer[IzLoggingRouter] = {
    ZLayer.succeed {
      new IzLoggingRouter {
        override def create(threshold: Log.Level, sinks: Seq[LogSink], buffer: LogQueue): LogRouter = {
          ConfigurableLogRouter(threshold = threshold, sinks = sinks, buffer)
        }
      }
    }
  }
}
