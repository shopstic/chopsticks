package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.DbApi
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool, KvdbTestSuite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Scope, ZIO}

abstract class KvdbDatabaseApiTest
    extends AsyncWordSpecLike
    with Matchers
    with KvdbTestSuite {
  protected def managedDb: ZIO[ZAkkaAppEnv with KvdbIoThreadPool with KvdbSerdesThreadPool with Scope, Throwable, DbApi]
  protected def dbMat: TestDatabase.Materialization
//  protected def anotherCf: AnotherCf1

  private lazy val withDb =
    createTestRunner[ZAkkaAppEnv with KvdbIoThreadPool with KvdbSerdesThreadPool, DbApi](managedDb) { effect =>
      effect.provideSome[ZAkkaAppEnv](
        KvdbIoThreadPool.live,
        KvdbSerdesThreadPool.fromDefaultPekkoDispatcher()
      )
    }

//  "open" should {
//    "work" in withDb { db =>
//      db.openTask()
//        .map(_ => assert(true))
//    }
//  }

  "withOptions" should {
    "update options of the underlying db clientOptions" in withDb { db =>
      ZIO.attempt {
        import scala.concurrent.duration._
        val newDb = db.withOptions(_.copy(watchTimeout = 123.seconds))
        newDb.options.watchTimeout shouldBe 123.seconds
        newDb.db.clientOptions.watchTimeout shouldBe 123.seconds
      }
    }
  }

  "KvdbColumnFamilyApi withOptions" should {
    "update options of the underlying db clientOptions" in withDb { db =>
      ZIO.attempt {
        import scala.concurrent.duration._
        val api = db.columnFamily(dbMat.plain).withOptions(_.copy(watchTimeout = 123.seconds))
        api.options.watchTimeout shouldBe 123.seconds
        api.db.clientOptions.watchTimeout shouldBe 123.seconds
      }
    }
  }

  "columnFamily" when {
    "getTask" should {
      "work with db" in withDb { db =>
        val cf = db.columnFamily(dbMat.plain)
        for {
          _ <- cf.putTask("foo", "bar")
          v <- cf.getValueTask(_ is "foo")
        } yield {
          v should equal(Some("bar"))
        }
      }

      "work with cf" in withDb { db =>
        for {
          cf <- ZIO.succeed {
            db.columnFamily(dbMat.plain)
          }
          _ <- cf.putTask("foo", "bar")
          v <- cf.getValueTask(_ is "foo")
        } yield {
          v should equal(Some("bar"))
        }
      }
    }
  }
}
