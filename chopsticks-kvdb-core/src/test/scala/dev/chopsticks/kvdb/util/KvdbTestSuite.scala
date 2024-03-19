package dev.chopsticks.kvdb.util

import better.files.File
import dev.chopsticks.fp.ZScalatestSuite
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.BaseCf
import org.scalatest.{Assertion, Suite}
import zio.{RIO, Scope, Unsafe, ZIO}

import scala.concurrent.Future

object KvdbTestSuite {
  def managedTempDir: ZIO[Scope, Throwable, File] = {
    ZIO.acquireRelease {
      ZIO.attemptBlocking(File.newTemporaryDirectory().deleteOnExit())
    } { f => ZIO.attemptBlocking(f.delete()).orDie }
  }
}

trait KvdbTestSuite extends ZScalatestSuite {
  this: Suite =>

  def createTestRunner[R <: ZAkkaAppEnv, Db](
    managedDb: ZIO[R with Scope, Throwable, Db]
  )(inject: RIO[R, Assertion] => RIO[ZAkkaAppEnv, Assertion]): (Db => RIO[R, Assertion]) => Future[Assertion] = {
    (testCode: Db => RIO[R, Assertion]) =>
      {
        Unsafe.unsafe { implicit unsafe =>
          bootstrapRuntime.unsafe.runToFuture {
            inject(
              ZIO.scoped[R] {
                managedDb
                  .flatMap(db => testCode(db).interruptAllChildrenPar)
              }
            ).provideEnvironment(appEnv)
          }
        }
      }
  }

  def populateColumn[CF <: BaseCf[K, V], K, V](
    db: TestDatabase.Db,
    column: CF,
    pairs: Seq[(K, V)]
  ): RIO[IzLogging, Unit] = {
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)
  }
}
