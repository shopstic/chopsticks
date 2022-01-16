package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.kvdb.KvdbDatabaseTest
import dev.chopsticks.kvdb.util.KvdbException.ConditionalTransactionFailedException
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesUtils, KvdbTestSuite}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Promise, ZIO}

final class SpecificRocksdbDatabaseTest
    extends AsyncWordSpecLike
    with Matchers
    with Inside
    with KvdbTestSuite {
  import KvdbDatabaseTest._

  private val dbMat = RocksdbDatabaseTest.dbMaterialization

  private lazy val defaultCf = dbMat.plain

  private lazy val withDb = createTestRunner(RocksdbDatabaseTest.managedDb) { effect =>
    import zio.magic._

    effect.injectSome[ZAkkaAppEnv](
      KvdbIoThreadPool.live
    )
  }

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
