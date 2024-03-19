//package dev.chopsticks.testkit
//
//import java.util.concurrent.atomic.AtomicLong
//import org.apache.pekko.actor.testkit.typed.scaladsl.ManualTime
//import com.typesafe.config.{Config, ConfigFactory}
//import org.apache.pekko.actor.typed
//import org.apache.pekko.actor.typed.scaladsl.adapter._
//import zio.internal.Platform
//import zio.test.TestClock
//import zio.Runtime
//
//import scala.concurrent.duration.FiniteDuration
//
//object ManualTimeAkkaTestKit {
//  final class ManualClock(mockClock: Option[TestClock] = None)(implicit typedSystem: typed.ActorSystem[_]) {
//    private val controller = ManualTime()
//    private val totalNanoPassed = new AtomicLong()
//    private val rt = Runtime[Any]((), Platform.fromExecutionContext(typedSystem.executionContext))
//
//    def timePasses(amount: FiniteDuration): Unit = {
//      val _ = totalNanoPassed.getAndAdd(amount.toNanos)
//      mockClock.foreach { c =>
//        val d = zio.duration.Duration.fromScala(amount)
//        rt.unsafeRun(c.adjust(d))
//      }
//      controller.timePasses(amount)
//    }
//
//    def nanoTime(): Long = totalNanoPassed.get()
//  }
//}
//
//trait ManualTimeAkkaTestKit extends AkkaTestKit {
//  override lazy val typesafeConfig: Config = {
//    val cfg = ManualTime.config.withFallback(ConfigFactory.load())
//    assert(
//      cfg.getBoolean("pekko.stream.materializer.debug.fuzzing-mode"),
//      "pekko.stream.materializer.debug.fuzzing-mode is not 'on' for testing, config loading is not working properly?"
//    )
//    cfg
//  }
//  implicit lazy val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
//}
