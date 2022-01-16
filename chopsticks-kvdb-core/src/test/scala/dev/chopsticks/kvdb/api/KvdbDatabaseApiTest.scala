package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.DbApi
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbSerdesThreadPool, KvdbTestSuite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Task, UIO, ZManaged}

abstract class KvdbDatabaseApiTest
    extends AsyncWordSpecLike
    with Matchers
    with KvdbTestSuite {
  protected def managedDb: ZManaged[ZAkkaAppEnv with KvdbIoThreadPool with KvdbSerdesThreadPool, Throwable, DbApi]
  protected def dbMat: TestDatabase.Materialization
//  protected def anotherCf: AnotherCf1

  private lazy val withDb = createTestRunner(managedDb) { effect =>
    import zio.magic._

    effect.injectSome[ZAkkaAppEnv](
      KvdbIoThreadPool.live,
      KvdbSerdesThreadPool.fromDefaultAkkaDispatcher()
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
      Task {
        import scala.concurrent.duration._
        val newDb = db.withOptions(_.copy(watchTimeout = 123.seconds))
        newDb.options.watchTimeout shouldBe 123.seconds
        newDb.db.clientOptions.watchTimeout shouldBe 123.seconds
      }
    }
  }

  "KvdbColumnFamilyApi withOptions" should {
    "update options of the underlying db clientOptions" in withDb { db =>
      Task {
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
          cf <- UIO {
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
