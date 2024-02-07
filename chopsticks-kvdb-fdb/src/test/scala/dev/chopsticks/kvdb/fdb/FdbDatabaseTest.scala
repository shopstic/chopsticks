package dev.chopsticks.kvdb.fdb

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, CounterCf, LookupCf, MaxCf, MinCf, PlainCf}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import zio.{RManaged, ZIO, ZManaged}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.codec.little_endian.{longValueSerdes, yearMonthValueSerdes}
import java.util.UUID

object FdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization with FdbMaterialization[TestDatabase.BaseCf] {
    object plain extends PlainCf
    object lookup extends LookupCf
    object counter extends CounterCf
    object min extends MinCf
    object max extends MaxCf

    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf].of(plain).and(lookup).and(counter).and(min).and(max)
    }
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampKey = Set.empty
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampValue = Set.empty
  }

  val managedDb: RManaged[ZAkkaAppEnv with KvdbIoThreadPool, FdbDatabase[
    TestDatabase.BaseCf,
    TestDatabase.CfSet
  ]] = {
    for {
      logger <- IzLogging.zioLogger.toManaged_
      rootDirectoryPath <- ZManaged.succeed(s"chopsticks-test-${UUID.randomUUID()}")
      _ <- logger.info(s"Using $rootDirectoryPath").toManaged_
      database <- FdbDatabase.manage(
        dbMaterialization,
        FdbDatabase.FdbDatabaseConfig(
          clusterFilePath = sys.env.get("FDB_CLUSTER_FILE").orElse(Some(sys.env("HOME") + "/.config/fdb/cluster.file")),
          rootDirectoryPath = rootDirectoryPath,
          stopNetworkOnClose = false
        )
      )
      _ <- ZManaged.make(ZIO.succeed(database)) { db =>
        ZIO.foreachPar_(dbMaterialization.columnFamilySet.value) { column => db.dropColumnFamily(column).orDie }
      }
    } yield database.asInstanceOf[FdbDatabase[TestDatabase.BaseCf, TestDatabase.CfSet]]
  }
}

final class FdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat = FdbDatabaseTest.dbMaterialization
  protected val managedDb = FdbDatabaseTest.managedDb
}
