package com.shopstic.testkit

import java.util.concurrent.atomic.AtomicLong

import akka.actor.testkit.typed.scaladsl.ManualTime
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed
import akka.actor.typed.scaladsl.adapter._

import scala.concurrent.duration.FiniteDuration

object ManualTimeAkkaTestKit {
  final class ManualClock(implicit typedSystem: typed.ActorSystem[_]) {
    private val controller = ManualTime()
    private val totalTimePassed = new AtomicLong()
    def timePasses(amount: FiniteDuration): Unit = {
      val _ = totalTimePassed.getAndAdd(amount.toNanos)
      controller.timePasses(amount)
    }

    def nanoTime(): Long = totalTimePassed.get()
  }
}

trait ManualTimeAkkaTestKit extends AkkaTestKit {
  override lazy val typesafeConfig: Config = {
    val cfg = ManualTime.config.withFallback(ConfigFactory.load())
    assert(
      cfg.getBoolean("akka.stream.materializer.debug.fuzzing-mode"),
      "akka.stream.materializer.debug.fuzzing-mode is not 'on' for testing, config loading is not working properly?"
    )
    cfg
  }
  implicit lazy val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
}
