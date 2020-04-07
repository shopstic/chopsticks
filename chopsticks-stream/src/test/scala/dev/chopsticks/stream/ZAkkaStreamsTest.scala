package dev.chopsticks.stream

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{Attributes, KillSwitches}
import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.stream.ZAkkaStreams.ops._
import dev.chopsticks.testkit.ManualTimeAkkaTestKit.ManualClock
import dev.chopsticks.testkit.{AkkaTestKitAutoShutDown, FixedTestClock, ManualTimeAkkaTestKit}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpecLike
import org.scalatest.{Assertion, Succeeded}
import zio.blocking._
import zio.clock.Clock
import zio.test.environment.TestClock
import zio.{CancelableFuture, Task, UIO, ZIO}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

final class ZAkkaStreamsTest
    extends ManualTimeAkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown
    with ScalaFutures {
  type Env = AkkaEnv with TestClock with Clock with Blocking

  private def runToFutureWithRuntime(run: ZIO[Env, Throwable, Assertion]): CancelableFuture[Assertion] = {
    val testClock = zio.ZEnv.live >>> zio.test.environment.Live.default >>> FixedTestClock.live
    val env = testClock ++ AkkaEnv.live(system) ++ Blocking.live ++ LogEnv.live
    val rt = AkkaApp.createRuntime(env)
    rt.unsafeRunToFuture(run)
  }

  def withRuntime(test: zio.Runtime[Env] => Assertion): Future[Assertion] = {
    runToFutureWithRuntime(ZIO.runtime[Env].flatMap { env => UIO(test(env)) })
  }

  def withEffect[E, A](test: ZIO[Env, Throwable, Assertion]): Future[Assertion] = {
    runToFutureWithRuntime(test)
  }

  "interruptibleLazySource" should {
    "interrupt effect" in withEffect {
      for {
        clock <- ZIO.access[TestClock](_.get[TestClock.Service]).map(c => new ManualClock(Some(c)))
        startP <- zio.Promise.make[Nothing, Unit]
        startFuture <- startP.await.toFuture
        interruptedP <- zio.Promise.make[Nothing, Unit]
        interruptedFuture <- interruptedP.await.toFuture
        source <- ZAkkaStreams.interruptibleLazySource {
          startP.succeed(()) *> ZIO
            .succeed(1)
            .delay(zio.duration.Duration(3, TimeUnit.SECONDS))
            .onInterrupt(interruptedP.succeed(()))
        }
        _ <- blocking(Task {
          val ks = source
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.ignore)(Keep.left)
            .run

          whenReady(startFuture, Timeout(Span(1, Seconds)))(identity)
          ks.shutdown()
          clock.timePasses(3.seconds)
          whenReady(interruptedFuture, Timeout(Span(1, Seconds)))(identity)
        })
      } yield Succeeded
    }
  }

  "manual time" in withRuntime { implicit rt =>
    val clock = new ManualClock(Some(rt.unsafeRun(ZIO.access[TestClock](_.get[TestClock.Service]))))

    val (source, sink) = TestSource
      .probe[Int]
      .effectMapAsync(1) { i =>
        ZIO.access[Clock](_.get[Clock.Service]).flatMap { clock =>
          clock.sleep(zio.duration.Duration(10, TimeUnit.SECONDS)) *> UIO(i + 1)
        }
      }
      .withAttributes(Attributes.inputBuffer(initial = 0, max = 1))
      .toMat(TestSink.probe[Int])(Keep.both)
      .run

    sink.request(2)
    source.sendNext(1)
    source.sendNext(2)
    sink.ensureSubscription()
    sink.expectNoMessage(100.millis)
    clock.timePasses(9.seconds)
    sink.expectNoMessage(100.millis)
    clock.timePasses(1.second)
    sink.expectNext(2)
    sink.expectNoMessage(100.millis)
    clock.timePasses(10.seconds)
    sink.expectNext(3)

    source.sendComplete()
    sink.expectComplete()

    Succeeded
  }

  "ZAkkaStreams" when {
    "recursiveSource" should {
      "repeat" in withRuntime { implicit env =>
        val sink = ZAkkaStreams
          .recursiveSource(ZIO.succeed(1), (_: Int, o: Int) => o) { s => Source(s to s + 2) }
          .toMat(TestSink.probe[Int])(Keep.right)
          .run()

        val sub = sink.expectSubscription()
        sub.request(6)
        sink.expectNextN(List(1, 2, 3, 3, 4, 5))
        sub.cancel()

        Succeeded
      }
    }
  }

  "ops" when {
    "switchFlatMapConcat" should {
      "function identically to flatMapConcat when there's no switching" in withRuntime { implicit env =>
        val (source, sink) = TestSource
          .probe[Source[Int, Any]]
          .switchFlatMapConcat(UIO(_))
          .toMat(TestSink.probe)(Keep.both)
          .run

        source.sendNext(Source(1 to 3))
        sink.request(3)
        sink.expectNextN(Vector(1, 2, 3))

        source.sendNext(Source(4 to 6))
        sink.request(3)
        sink.expectNextN(Vector(4, 5, 6))

        source.sendComplete()
        sink.expectComplete()

        Succeeded
      }

      "cancel the prior source and switch to the new one" in withRuntime { implicit rt =>
        val akkaService = rt.environment.get[AkkaEnv.Service]

        val promise = Promise[Boolean]()
        val (source, sink) = TestSource
          .probe[Source[Int, Any]]
          .switchFlatMapConcat(UIO(_))
          .toMat(TestSink.probe)(Keep.both)
          .run

        sink.request(2)
        source.sendNext {
          Source
            .future(
              akka.pattern
                .after(3.seconds, akkaService.actorSystem.scheduler)(Future.successful(1))(akkaService.dispatcher)
            )
            .watchTermination() { (_, f) =>
              f.onComplete(_ => promise.success(true))(akkaService.dispatcher)
              f
            }
        }

        source.sendNext(Source.single(2))
        sink.expectNext(2)

        whenReady(promise.future) { r => r should be(true) }

        source.sendComplete()
        sink.expectComplete()

        Succeeded
      }
    }
  }
}
