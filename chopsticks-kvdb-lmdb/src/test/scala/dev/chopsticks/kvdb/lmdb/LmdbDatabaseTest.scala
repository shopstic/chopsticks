package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, DefaultCf, LookupCf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabase, KvdbDatabaseTest, TestDatabase}
import eu.timepit.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import squants.information.InformationConversions._
import zio.ZManaged

object LmdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization {
    object default extends DefaultCf
    object lookup extends LookupCf
    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of default and lookup
    }
  }

  val managedDb: ZManaged[AkkaApp.Env, Throwable, TestDatabase.Db] = {
    for {
      dir <- KvdbTestUtils.managedTempDir
      db <- KvdbDatabase.manage(
        LmdbDatabase(
          dbMaterialization,
          LmdbDatabase.Config(
            path = refineV[NonEmpty](dir.pathAsString).right.get,
            maxSize = 64.mib,
            noSync = false,
            ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
          )
        )
      )
    } yield db
  }
}

final class LmdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat = LmdbDatabaseTest.dbMaterialization
  protected val managedDb = LmdbDatabaseTest.managedDb
}
