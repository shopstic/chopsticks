package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.{AkkaApp, AkkaEnv, LoggingContext}
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.DbApi
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.{AsyncWordSpecLike, Matchers}
import zio.ZManaged

abstract class KvdbDatabaseApiTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown
    with LoggingContext {

  protected def managedDb: ZManaged[AkkaApp.Env, Throwable, DbApi]
  protected def dbMat: TestDatabase.Materialization
//  protected def anotherCf: AnotherCf1

  private lazy val withDb = KvdbTestUtils.createTestRunner(Environment, managedDb)
  private lazy val withCf = KvdbTestUtils.createTestRunner(Environment, managedDb.map(_.columnFamily(dbMat.plain)))

  private lazy val as = system

  private object Environment extends AkkaApp.LiveEnv {
    val akkaService: AkkaEnv.Service = AkkaEnv.Service.fromActorSystem(as)
  }

  "open" should {
    "work" in withDb { db =>
      db.openTask()
        .map(_ => assert(true))
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
