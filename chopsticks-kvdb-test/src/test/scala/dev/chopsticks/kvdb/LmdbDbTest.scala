package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.DbTest.DbTest
import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import org.scalatest.Assertion
import zio.{Task, RIO, UIO, ZIO}

class LmdbDbTest extends DbTest {
  protected val runTest: (DbInterface[TestDb.type] => Task[Assertion]) => RIO[AkkaApp.Env, Assertion] =
    (test: DbInterface[TestDb.type] => Task[Assertion]) => {
      DbTest.withTempDir { dir =>
        ZIO
          .access[AkkaEnv] { implicit env =>
            LmdbDb[TestDb.type](
              TestDb,
              dir.pathAsString,
              maxSize = 64 << 20,
              noSync = false,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
          .bracket(db => db.closeTask().catchAll(_ => UIO.unit)) { db =>
            test(db)
          }
      }
    }
}
