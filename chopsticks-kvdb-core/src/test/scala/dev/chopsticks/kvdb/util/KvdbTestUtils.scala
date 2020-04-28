package dev.chopsticks.kvdb.util

import better.files.File
import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.TestDatabase
import dev.chopsticks.kvdb.TestDatabase.BaseCf
import org.scalatest.Assertion
import zio.blocking.{blocking, Blocking}
import zio.{RIO, Task, ZManaged}

import scala.concurrent.Future

object KvdbTestUtils {
  def managedTempDir: ZManaged[Blocking, Throwable, File] = {
    ZManaged.make {
      blocking(Task(File.newTemporaryDirectory().deleteOnExit()))
    } { f => blocking(Task(f.delete())).orDie }
  }

  def createTestRunner[Db](
    managed: ZManaged[AkkaApp.Env, Throwable, Db]
  )(implicit rt: zio.Runtime[AkkaApp.Env]): (Db => RIO[AkkaApp.Env, Assertion]) => Future[Assertion] = {
    (testCode: Db => RIO[AkkaApp.Env, Assertion]) =>
      managed
        .use(testCode(_))
        .unsafeRunToFuture
  }

  def populateColumn[CF <: BaseCf[K, V], K, V](
    db: TestDatabase.Db,
    column: CF,
    pairs: Seq[(K, V)]
  ): Task[Unit] = {
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)
  }
}
