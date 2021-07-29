package dev.chopsticks.kvdb.fdb

import akka.testkit.ImplicitSender
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.tuple.ByteArrayUtil
import dev.chopsticks.fp.AkkaDiApp
import dev.chopsticks.fp.iz_logging.IzLogging.IzLoggingConfig
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.kvdb.KvdbDatabaseTest
import dev.chopsticks.kvdb.util.KvdbException.ConditionalTransactionFailedException
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesUtils, KvdbTestUtils}
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import izumi.logstage.api.IzLogger.Level
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Promise, ZIO}

import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

final class SpecificFdbDatabaseTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inside
    with ImplicitSender
    with AkkaTestKitAutoShutDown {
  import KvdbDatabaseTest._

  private val dbMat = FdbDatabaseTest.dbMaterialization

  private lazy val defaultCf = dbMat.plain

  private val izLoggingConfig = IzLoggingConfig(level = Level.Info, noColor = false, jsonFileSink = None)

  private lazy val runtime = AkkaDiApp.createRuntime(AkkaDiApp.Env.live ++ (IzLoggingRouter.live >>> IzLogging.live(
    typesafeConfig
  )))
  private lazy val runtimeLayer =
    ((IzLoggingRouter.live >>> IzLogging.live(izLoggingConfig)) ++ AkkaDiApp.Env.live) >+> KvdbIoThreadPool.live
  private lazy val withDb = KvdbTestUtils.createTestRunner(FdbDatabaseTest.managedDb, runtimeLayer)(runtime)

  "system metadataVersion key" should {
    "enforce conflicts" in withDb { db =>
      val METADATA_VERSION_KEY: Array[Byte] =
        ByteArrayUtil.join(Array[Byte](0xFF.toByte), "/metadataVersion".getBytes(StandardCharsets.US_ASCII))

      for {
        lock1 <- Promise.make[Nothing, Unit]
        lock2 <- Promise.make[Nothing, Unit]
        rt <- ZIO.runtime[Any]
        counter = new AtomicLong()
        version1Fib <- db
          .write(
            "one",
            api => {
              import scala.jdk.FutureConverters._
              api.tx.get(METADATA_VERSION_KEY)
                .thenCompose { version =>
                  counter.incrementAndGet()
                  api.deletePrefix(defaultCf, Array.emptyByteArray)
                  rt.unsafeRunToFuture(lock1.succeed(()) *> lock2.await.as(version)).asJava
                }
            }
          )
          .fork
        _ <- lock1.await
        _ <- db.write(
          "two",
          api => {
            val value = Array.fill[Byte](14)(0x00.toByte)

            api.tx.mutate(
              MutationType.SET_VERSIONSTAMPED_VALUE,
              METADATA_VERSION_KEY,
              value
            )

            CompletableFuture.completedFuture(())
          }
        )
        version2 <- db.read(_.tx.get(METADATA_VERSION_KEY))
        _ <- lock2.succeed(())
        version1 <- version1Fib.join
      } yield {
        ByteArrayUtil.compareUnsigned(version1, version2) shouldBe (0)
        counter.get() shouldBe (2)
      }
    }
  }

  "conditionalTransactionTask" should {
    "fail upon conflict" in withDb { db =>
      for {
        _ <- db.putTask(defaultCf, "aaaa", "aaaa")
        promise <- Promise.make[Nothing, Unit]
        rt <- ZIO.runtime[Any]
        counter = new AtomicLong()
        fib <- db
          .conditionalTransactionTask(
            reads = db
              .readTransactionBuilder()
              .get(defaultCf, "aaaa")
              .result,
            condition = test => {
              val _ = counter.getAndIncrement()
              rt.unsafeRun(promise.await)
              test match {
                case head :: Nil if head.exists(p => KvdbSerdesUtils.byteArrayToString(p._2) == "aaaa") =>
                  true
                case _ =>
                  false
              }
            },
            actions = db
              .transactionBuilder()
              .delete(defaultCf, "aaaa")
              .result
          )
          .fork
        _ <- db.putTask(defaultCf, "aaaa", "bbbb")
        _ <- promise.succeed(())
        txRet <- fib.join.either
        ret <- db.getTask(defaultCf, $(_ is "aaaa"))
      } yield {
        counter.get() should be(2)
        txRet should matchPattern {
          case Left(ConditionalTransactionFailedException(_)) =>
        }
        assertPair(ret, "aaaa", "bbbb")
      }
    }
  }
}
