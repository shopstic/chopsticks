package dev.chopsticks.kvdb.util

import java.nio.file.Files
import dev.chopsticks.fp.zio_ext.*
import dev.chopsticks.kvdb.{ColumnFamily, TestDatabase}
import dev.chopsticks.testkit.ZScalatestSuite
import org.scalatest.{Assertion, Suite}
import zio.*

import java.io.File
import scala.concurrent.Future

object KvdbTestSuite:
  def managedTempDir: RIO[Scope, File] =
    ZIO
      .acquireRelease {
        ZIO
          .blocking(ZIO.attempt(Files.createTempDirectory("")))
          .map(_.toFile)
      } { f =>
        ZIO.blocking(ZIO.attempt(deleteFileUnsafe(f))).orDie
      }

  private def deleteFileUnsafe(f: File): Unit =
    if (f.isDirectory) f.listFiles().toList.foreach(deleteFileUnsafe)
    Files.delete(f.toPath)

end KvdbTestSuite

trait KvdbTestSuite extends ZScalatestSuite:
  this: Suite =>

  def createTestRunner[R, Db](
    managedDb: ZIO[R, Throwable, Db]
  )(inject: RIO[R, Assertion] => Task[Assertion]): (Db => RIO[R, Assertion]) => Future[Assertion] =
    (testCode: Db => RIO[R, Assertion]) =>
      Unsafe.unsafe { implicit unsafe =>
        bootstrapRuntime.unsafe.runToFuture {
          inject {
            managedDb.flatMap(testCode).interruptAllChildrenPar
          }
        }
      }

  def populateColumn[CF >: TestDatabase.CfSet <: ColumnFamily[_, _]](
    db: TestDatabase.Db,
    column: CF,
    pairs: Seq[(column.Key, column.Value)]
  ): Task[Unit] =
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)

end KvdbTestSuite
