package dev.chopsticks.fp

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import zio.{Has, ZLayer}

package object log_env {
  type LogEnv = Has[LogEnv.Service]

  object LogEnv extends Serializable {
    trait Service extends Serializable {
      def logger(implicit ctx: LogCtx): Logger
    }

    val any: ZLayer[LogEnv, Nothing, LogEnv] =
      ZLayer.requires[LogEnv]

    val live: ZLayer[Any, Nothing, LogEnv] = ZLayer.succeed { (ctx: LogCtx) =>
      Logger(LoggerFactory.getLogger(ctx.name))
    }
  }
}
