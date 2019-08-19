package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.DbTest.DbTest
import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions
import org.scalatest.Assertion
import zio.{RIO, Task, UIO, ZIO}

class RocksdbLocalDbTest extends DbTest {
  protected val runTest: (DbInterface[TestDb.type] => Task[Assertion]) => RIO[AkkaApp.Env, Assertion] =
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
              checksumOnRead = true,
              syncWriteBatch = true,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
          .bracket(db => db.closeTask().catchAll(_ => UIO.unit)) { db =>
            test(db)
          }
      }
    }
}
