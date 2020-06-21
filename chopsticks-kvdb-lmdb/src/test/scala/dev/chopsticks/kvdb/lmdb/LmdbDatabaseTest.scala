package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, LookupCf, PlainCf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbTestUtils}
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import squants.information.InformationConversions._
import zio.ZManaged

import scala.concurrent.duration._

object LmdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization {
    object plain extends PlainCf
    object lookup extends LookupCf
    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of plain and lookup
    }
  }

  val managedDb: ZManaged[AkkaApp.Env with KvdbIoThreadPool, Throwable, TestDatabase.Db] = {
    for {
      dir <- KvdbTestUtils.managedTempDir
      db <- LmdbDatabase.manage(
        dbMaterialization,
        LmdbDatabase.LmdbDatabaseConfig(
          path = NonEmptyString.unsafeFrom(dir.pathAsString),
          maxSize = 64.mib,
          noSync = false,
          ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher",
          clientOptions = KvdbClientOptions(tailPollingMaxInterval = 10.millis)
        )
      )
    } yield db
  }
}

final class LmdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat = LmdbDatabaseTest.dbMaterialization
  protected val managedDb = LmdbDatabaseTest.managedDb
}
