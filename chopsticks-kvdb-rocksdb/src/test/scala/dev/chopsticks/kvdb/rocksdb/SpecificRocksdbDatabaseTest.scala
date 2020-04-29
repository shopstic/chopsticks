package dev.chopsticks.kvdb.rocksdb

import akka.testkit.ImplicitSender
import dev.chopsticks.fp.{AkkaApp, LoggingContext}
import dev.chopsticks.kvdb.{KvdbDatabaseTest, TestDatabase}
import dev.chopsticks.kvdb.util.{KvdbSerdesUtils, KvdbTestUtils}
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbException.ConditionalTransactionFailedException
import zio.{Promise, ZIO}

final class SpecificRocksdbDatabaseTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inside
    with ImplicitSender
    with AkkaTestKitAutoShutDown
    with LoggingContext {
  import KvdbDatabaseTest._

  private val dbMat = RocksdbDatabaseTest.dbMaterialization

  private lazy val defaultCf = dbMat.plain

  private lazy val runtime = AkkaApp.createRuntime(AkkaApp.Env.live)
  private lazy val withDb = KvdbTestUtils.createTestRunner[TestDatabase.Db](RocksdbDatabaseTest.managedDb)(runtime)

  "conditionalTransactionTask" should {
    "fail upon conflict" in withDb { db =>
      for {
        _ <- db.putTask(defaultCf, "aaaa", "aaaa")
        conflictStart <- Promise.make[Nothing, Unit]
        conflictEnd <- Promise.make[Nothing, Unit]
        rt <- ZIO.runtime[Any]
        fib <- db
          .conditionalTransactionTask(
            db.readTransactionBuilder()
              .get(defaultCf, "aaaa")
              .result,
            test => {
              rt.unsafeRun(conflictStart.succeed(()))
              rt.unsafeRun(conflictEnd.await)

              test match {
                case head :: Nil if head.exists(p => KvdbSerdesUtils.byteArrayToString(p._2) == "aaaa") => true
                case _ => false
              }
            },
            db.transactionBuilder()
              .delete(defaultCf, "aaaa")
              .result
          )
          .fork
        _ <- conflictStart.await
        _ <- db.putTask(defaultCf, "aaaa", "bbbb")
        _ <- conflictEnd.succeed(())
        txRet <- fib.join.either
        ret <- db.getTask(defaultCf, $(_ is "aaaa"))
      } yield {
        txRet should matchPattern {
          case Left(ConditionalTransactionFailedException(_)) =>
        }
        assertPair(ret, "aaaa", "bbbb")
      }
    }
  }
}
