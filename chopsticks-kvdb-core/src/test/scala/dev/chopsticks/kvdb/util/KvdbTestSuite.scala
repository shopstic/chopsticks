package dev.chopsticks.kvdb.util

import better.files.File
import dev.chopsticks.fp.ZScalatestSuite
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.BaseCf
import org.scalatest.{Assertion, Suite}
import zio.blocking.{blocking, Blocking}
import zio.{RIO, Task, ZManaged}

import scala.concurrent.Future

object KvdbTestSuite {
  def managedTempDir: ZManaged[Blocking, Throwable, File] = {
    ZManaged.make {
      blocking(Task(File.newTemporaryDirectory().deleteOnExit()))
    } { f => blocking(Task(f.delete())).orDie }
  }
}

trait KvdbTestSuite extends ZScalatestSuite {
  this: Suite =>

  def createTestRunner[R <: ZAkkaAppEnv, Db](
    managedDb: ZManaged[R, Throwable, Db]
  )(inject: RIO[R, Assertion] => RIO[ZAkkaAppEnv, Assertion]): (Db => RIO[R, Assertion]) => Future[Assertion] = {
    (testCode: Db => RIO[R, Assertion]) =>
      {
        bootstrapRuntime
          .unsafeRunToFuture(
            inject(managedDb
              .use(testCode(_).interruptAllChildrenPar))
              .provide(appEnv)
          )
      }
  }

  def populateColumn[CF <: BaseCf[K, V], K, V](
    db: TestDatabase.Db,
    column: CF,
    pairs: Seq[(K, V)]
  ): RIO[MeasuredLogging, Unit] = {
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)
  }
}
