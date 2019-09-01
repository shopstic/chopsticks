package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestDb, TestDbCf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.kvdb.util.KvdbUtils.KvdbAlreadyClosedException
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest}
import zio.{ZIO, ZManaged}

object LmdbDatabaseTest {
  object defaultCf extends DefaultCf
  object lookupCf extends LookupCf
  //noinspection TypeAnnotation
  private val cfs = ColumnFamilySet[TestDbCf] of defaultCf and lookupCf
  val managedDb: ZManaged[AkkaApp.Env, Throwable, TestDb] = {
    for {
      dir <- KvdbTestUtils.managedTempDir
      db <- ZManaged.make {
        ZIO
          .access[AkkaApp.Env] { implicit env =>
            LmdbDatabase(
              cfs,
              dir.pathAsString,
              maxSize = 64 << 20,
              noSync = false,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
      } {
        _.closeTask().catchAll {
          case _: KvdbAlreadyClosedException => ZIO.unit
          case t => ZIO.die(t)
        }
      }
    } yield db
  }
}

final class LmdbDatabaseTest extends KvdbDatabaseTest {
  protected val defaultCf = LmdbDatabaseTest.defaultCf
  protected val lookupCf = LmdbDatabaseTest.lookupCf
  protected val managedDb = LmdbDatabaseTest.managedDb
}
