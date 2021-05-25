package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.fp.AkkaDiApp
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.DbApi
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbTestUtils}
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.{Task, ZManaged}

abstract class KvdbDatabaseApiTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  protected def managedDb: ZManaged[AkkaDiApp.Env with IzLogging with KvdbIoThreadPool, Throwable, DbApi]
  protected def dbMat: TestDatabase.Materialization
//  protected def anotherCf: AnotherCf1

  private lazy val loggingLayer = (IzLoggingRouter.live >>> IzLogging.live(
    typesafeConfig
  ))
  private lazy val runtime = AkkaDiApp.createRuntime(AkkaDiApp.Env.live ++ loggingLayer)
  private lazy val runtimeLayer = AkkaDiApp.Env.live >+> KvdbIoThreadPool.live() ++ loggingLayer
  private lazy val withDb = KvdbTestUtils.createTestRunner(managedDb, runtimeLayer)(runtime)
  private lazy val withCf = KvdbTestUtils.createTestRunner(
    managedDb.map(_.columnFamily(dbMat.plain)),
    runtimeLayer
  )(runtime)

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

      "work with cf" in withCf { cf =>
        for {
          _ <- cf.putTask("foo", "bar")
          v <- cf.getValueTask(_ is "foo")
        } yield {
          v should equal(Some("bar"))
        }
      }
    }
  }
}
