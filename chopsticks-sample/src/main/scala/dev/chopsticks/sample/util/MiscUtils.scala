package dev.chopsticks.sample.util

import zio.{Duration, UIO, ZIO}
import zio.stream.ZStream

import scala.collection.immutable.ListMap

object MiscUtils {
  def printKey(bs: Array[Byte]): String = {
    bs.map { b =>
      if (b > 32) b.toChar
      else String.format("\\x%02X", b)
    }
      .mkString("")
  }

  def logRates(interval: Duration)(collect: => ListMap[String, Double]): UIO[Unit] = {
    for {
      ret <- ZStream
        .tick(interval)
        .as(collect)
        .mapAccum(ListMap.empty[String, Double]) { (priorSnapshot, snap) =>
          if (priorSnapshot.isEmpty) (snap, None)
          else {
            val elapsed = priorSnapshot.map {
              case (pk, pv) =>
                pk -> (snap(pk) - pv)
            }
            (snap, Some(elapsed))
          }
        }
        .collect { case Some(elapsed) => elapsed }
        .runForeach { elapsed =>
          val result = elapsed.map { case (k, v) => s"$k=$v" }.mkString(" ")
          ZIO.logInfo(result)
        }
    } yield ret
  }
}
