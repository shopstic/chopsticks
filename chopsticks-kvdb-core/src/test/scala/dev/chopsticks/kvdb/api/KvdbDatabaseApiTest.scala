package dev.chopsticks.kvdb.api

import akka.actor.ActorSystem
import dev.chopsticks.fp.{AkkaApp, LoggingContext}
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestDbApi}
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.{AsyncWordSpecLike, Matchers}
import dev.chopsticks.kvdb.codec.primitive._
import zio.ZManaged

abstract class KvdbDatabaseApiTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown
    with LoggingContext {

  protected def managedDb: ZManaged[AkkaApp.Env, Throwable, TestDbApi]
  protected def defaultCf: DefaultCf
  protected def lookupCf: LookupCf

//  protected def anotherCf: AnotherCf1

  private lazy val withDb = KvdbTestUtils.createTestRunner(Environment, managedDb)
  private lazy val withCf = KvdbTestUtils.createTestRunner(Environment, managedDb.map(_.columnFamily(defaultCf)))

  private lazy val as = system

  private object Environment extends AkkaApp.LiveEnv {
    implicit val actorSystem: ActorSystem = as
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
        val cf = db.columnFamily(defaultCf)
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
