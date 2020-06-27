package dev.chopsticks.kvdb.fdb

import java.util.concurrent.atomic.AtomicLong

import akka.testkit.ImplicitSender
import dev.chopsticks.fp.{AkkaApp, LoggingContext}
import dev.chopsticks.kvdb.KvdbDatabaseTest
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbException.ConditionalTransactionFailedException
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesUtils, KvdbTestUtils}
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Promise, ZIO}

final class SpecificFdbDatabaseTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inside
    with ImplicitSender
    with AkkaTestKitAutoShutDown
    with LoggingContext {
  import KvdbDatabaseTest._

  private val dbMat = FdbDatabaseTest.dbMaterialization

  private lazy val defaultCf = dbMat.plain

  private lazy val runtime = AkkaApp.createRuntime(AkkaApp.Env.live ++ KvdbIoThreadPool.live())
  private lazy val withDb = KvdbTestUtils.createTestRunner(FdbDatabaseTest.managedDb)(runtime)

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
