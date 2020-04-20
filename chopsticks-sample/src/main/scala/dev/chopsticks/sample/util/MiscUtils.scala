package dev.chopsticks.sample.util

import akka.Done
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.fp.ZService
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.{LogCtx, LogEnv}
import dev.chopsticks.stream.ZAkkaStreams
import zio.RIO

import scala.collection.immutable.ListMap
import scala.concurrent.duration.{Duration, FiniteDuration}

object MiscUtils {
  def printKey(bs: Array[Byte]): String = {
    bs.map { b =>
        if (b > 32) b.toChar
        else String.format("\\x%02X", b)
      }
      .mkString("")
  }

  def logRates(interval: FiniteDuration)(collect: => ListMap[String, Double])(
    implicit logCtx: LogCtx
  ): RIO[AkkaEnv with LogEnv, Done] = {
    val graphTask = for {
      logger <- ZService[LogEnv.Service].map(_.logger)
    } yield {
      Source
        .tick(Duration.Zero, interval, ())
        .map { _ => collect }
        .statefulMapConcat(() => {
          var priorSnapshot = ListMap.empty[String, Double]

          snap => {
            if (priorSnapshot.isEmpty) {
              priorSnapshot = snap
              Nil
            }
            else {
              val elapsed = priorSnapshot.map {
                case (pk, pv) =>
                  pk -> (snap(pk) - pv)
              }
              priorSnapshot = snap
              List(elapsed)
            }
          }
        })
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach { elapsed =>
          logger.info(
            elapsed
              .map {
                case (k, v) =>
                  s"$k=$v"
              }
              .mkString(" ")
          )
        })(Keep.both)
    }

    ZAkkaStreams.interruptibleGraph(graphTask, graceful = true)
  }
}