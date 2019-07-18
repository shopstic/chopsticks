package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.DbTest.{DbTest, TestDb, TestDbColumn}
import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions
import org.scalatest.Assertion
import zio.{Task, TaskR, UIO, ZIO}

class RocksdbLocalDbTest extends DbTest {
  protected val runTest: (DbInterface[DbTest.TestDb.type] => Task[Assertion]) => TaskR[AkkaApp.Env, Assertion] =
    (test: DbInterface[TestDb.type] => Task[Assertion]) => {
      DbTest.withTempDir { dir =>
        ZIO
          .access[AkkaEnv] { implicit env =>
            RocksdbDb[TestDb.type](
              TestDb,
              dir.pathAsString,
              Map.empty[TestDbColumn[_, _], RocksdbCFOptions],
              readOnly = false,
              startWithBulkInserts = false,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
          .bracket(db => db.closeTask().catchAll(_ => UIO.unit)) { db =>
            test(db)
          }
      }
    }
}
