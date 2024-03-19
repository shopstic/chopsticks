package dev.chopsticks.sample.util

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import zio.{RIO, ZIO}

import scala.annotation.nowarn
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

  // todo [migration]
  @nowarn("cat=deprecation")
  def logRates(interval: FiniteDuration)(collect: => ListMap[String, Double])(implicit
    logCtx: LogCtx
  ): RIO[PekkoEnv with IzLogging, Done] = {
    for {
      logger <- ZIO.serviceWith[IzLogging](_.logger)
      ret <- Source
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
        .toZAkkaSource
        .killSwitch
        .interruptibleRunWith(Sink.foreach { elapsed =>
          logger.info(s"${elapsed
            .map {
              case (k, v) =>
                s"$k=$v"
            }
            .mkString(" ") -> "snapshot" -> null}")
        })
    } yield ret
  }
}
