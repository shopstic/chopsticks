package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestCf, TestDb}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbUtils.KvdbAlreadyClosedException
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest}
import org.scalatest.Assertion
import zio.{RIO, ZIO}

final class LmdbDatabaseTest extends KvdbDatabaseTest {
  protected object defaultCf extends DefaultCf
  protected object lookupCf extends LookupCf
  private val testDbCfs = ColumnFamilySet[TestCf] of defaultCf and lookupCf

  protected val runTest: (TestDb => RIO[AkkaApp.Env, Assertion]) => RIO[AkkaApp.Env, Assertion] =
    (test: TestDb => RIO[AkkaApp.Env, Assertion]) => {
      KvdbDatabaseTest.withTempDir { dir =>
        ZIO
          .access[AkkaEnv] { implicit env =>
            LmdbDatabase(
              testDbCfs,
              dir.pathAsString,
              maxSize = 64 << 20,
              noSync = false,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
          .bracket(
            db =>
              db.closeTask().catchAll {
                case _: KvdbAlreadyClosedException => ZIO.unit
                case t => ZIO.die(t)
              }
          ) { db =>
            test(db)
          }
      }
    }
}
