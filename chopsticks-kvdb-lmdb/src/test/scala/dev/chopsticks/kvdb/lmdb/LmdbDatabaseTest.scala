package dev.chopsticks.kvdb.lmdb

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, CounterCf, LookupCf, MaxCf, MinCf, PlainCf}
import dev.chopsticks.kvdb.codec.little_endian.{longValueSerdes, yearMonthValueSerdes}
import dev.chopsticks.kvdb.util.{KvdbIoThreadPool, KvdbTestSuite}
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import squants.information.InformationConversions._
import zio.ZManaged
import dev.chopsticks.kvdb.codec.primitive._
import scala.concurrent.duration._

object LmdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization {
    object plain extends PlainCf
    object lookup extends LookupCf
    object counter extends CounterCf
    object min extends MinCf
    object max extends MaxCf

    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of plain and lookup and counter and min and max
    }
  }

  val managedDb: ZManaged[ZAkkaAppEnv with KvdbIoThreadPool, Throwable, TestDatabase.Db] = {
    for {
      dir <- KvdbTestSuite.managedTempDir
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
