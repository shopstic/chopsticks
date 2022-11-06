package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.MutationType
import com.apple.foundationdb.tuple.ByteArrayUtil
import dev.chopsticks.fdb.transaction.ZFdbTransaction
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.kvdb.KvdbDatabaseTest
import dev.chopsticks.kvdb.util.KvdbException.ConditionalTransactionFailedException
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesUtils, KvdbTestSuite}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Promise, Task, UIO, ZIO, ZRef}

import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

final class SpecificFdbDatabaseTest
    extends AsyncWordSpecLike
    with Matchers
    with Inside
    with KvdbTestSuite {
  import KvdbDatabaseTest._

  private val dbMat = FdbDatabaseTest.dbMaterialization

  private lazy val defaultCf = dbMat.plain
  private lazy val counterCf = dbMat.counter

  private lazy val withDb =
    createTestRunner(FdbDatabaseTest.managedDb) { effect =>
      import zio.magic._

      effect.injectSome[ZAkkaAppEnv](
        KvdbIoThreadPool.live
      )
    }

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

  "ZIO-based low-level transaction" should {
    "work" in withDb { db =>
      for {
        transaction <- UIO(ZFdbTransaction(db))
        _ <- transaction.write { tx =>
          val cf = tx.keyspace(defaultCf)

          for {
            _ <- cf.get(_.is("foo")).tap(v => Task(v shouldBe empty))
            _ <- UIO(cf.put("foo", "bar"))
            _ <- cf.getValue(_.is("foo")).tap(v => Task(v shouldEqual Some("bar")))
            _ <- UIO(cf.put("foo", "baz"))
          } yield ()
        }
        _ <- transaction
          .write { tx =>
            val cf = tx.keyspace(defaultCf)

            for {
              _ <- cf.getValue(_.is("foo")).tap(v => Task(v shouldEqual Some("baz")))
              _ <- UIO(cf.put("foo", "boo"))
              _ <- Task.fail(new IllegalStateException("Test failure")).unit
            } yield ()
          }
          .ignore

        foo <- transaction.read { tx =>
          tx.keyspace(defaultCf).getValue(_.is("foo"))
        }
      } yield {
        foo shouldEqual Some("baz")
      }
    }

    "support read interruption" in withDb { db =>
      import zio.duration._
      for {
        transaction <- UIO(ZFdbTransaction(db))
        ref <- ZRef.make(false)
        fib <- transaction.read { tx =>
          tx.keyspace(defaultCf).getValue(_.is("foo")).delay(5.seconds).onInterrupt(ref.set(true))
        }.fork
        _ <- fib.interrupt.delay(1.second)
        innerInterrupted <- ref.get
      } yield {
        innerInterrupted shouldEqual true
      }
    }
  }

  "add mutation" should {
    "work with Long value" in withDb { db =>
      for {
        transaction <- UIO(ZFdbTransaction(db))
        _ <- transaction.write { tx =>
          Task {
            val cf = tx.keyspace(counterCf)

            cf.put("foo", Long.MaxValue - 1)
            cf.mutateAdd("foo", 1)
          }
        }
        currentPair <- transaction
          .read { tx =>
            val cf = tx.keyspace(counterCf)

            cf.get(_ is "foo")
          }
      } yield {
        currentPair shouldEqual Some("foo" -> Long.MaxValue)
      }
    }
  }
}
