package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.TestDatabase._
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.{PointLookupPattern, PrefixedScanPattern}
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import squants.information.InformationConversions._
import zio.ZManaged

import scala.concurrent.duration._

object RocksdbDatabaseTest {
  object dbMaterialization extends Materialization with RocksdbMaterialization[BaseCf, CfSet] {
    object plain extends PlainCf
    object lookup extends LookupCf

    val defaultColumnFamily: plain.type = plain

    val columnFamilyConfigMap: RocksdbColumnFamilyOptionsMap[BaseCf, CfSet] = {
      RocksdbColumnFamilyOptionsMap[BaseCf] of (
        plain,
        RocksdbColumnFamilyConfig(
          memoryBudget = 1.mib,
          blockCache = 1.mib,
          blockSize = 8.kib,
          writeBufferCount = 4
        ).toOptions(PrefixedScanPattern(1))
      ) and (
        lookup,
        RocksdbColumnFamilyConfig(
          memoryBudget = 1.mib,
          blockCache = 1.mib,
          blockSize = 8.kib,
          writeBufferCount = 4
        ).toOptions(PointLookupPattern)
      )
    }
    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = ColumnFamilySet[BaseCf] of plain and lookup
  }

  val managedDb: ZManaged[AkkaApp.Env, Throwable, Db] = {
    for {
      dir <- KvdbTestUtils.managedTempDir
      db <- RocksdbDatabase.manage(
        dbMaterialization,
        RocksdbDatabase.Config(
          path = NonEmptyString.unsafeFrom(dir.pathAsString),
          startWithBulkInserts = false,
          useDirectIo = true,
          ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher",
          clientOptions = KvdbClientOptions(tailPollingMaxInterval = 10.millis)
        )
      )
    } yield db
  }
}

final class RocksdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat = RocksdbDatabaseTest.dbMaterialization
  protected val managedDb = RocksdbDatabaseTest.managedDb
}
