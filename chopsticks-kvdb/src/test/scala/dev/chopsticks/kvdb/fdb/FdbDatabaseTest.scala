package dev.chopsticks.kvdb.fdb

import dev.chopsticks.kvdb.TestDatabase.{CfSet, CounterCf, LookupCf, PlainCf}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import zio.*

import java.util.UUID

object FdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization with FdbMaterialization {
    override val plain = new PlainCf {}
    override val lookup = new LookupCf {}
    override val counter = new CounterCf {}

    val columnFamilySet: ColumnFamilySet[CfSet] = {
      ColumnFamilySet.of(plain).and(lookup).and(counter)
    }
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampKey = Set.empty
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampValue = Set.empty
  }

  val managedDb: ZIO[KvdbIoThreadPool & Scope, Throwable, FdbDatabase[TestDatabase.CfSet]] = {
    for {
      rootDirectoryPath <- ZIO.succeed(s"chopsticks-test-${UUID.randomUUID()}")
      _ <- ZIO.logInfo(s"Using $rootDirectoryPath")
      database <- FdbDatabase.manage[TestDatabase.CfSet](
        dbMaterialization,
        FdbDatabase.FdbDatabaseConfig(
          clusterFilePath = sys.env.get("FDB_CLUSTER_FILE").orElse(Some(s"local-dev/fdb.cluster")),
          rootDirectoryPath = rootDirectoryPath,
          stopNetworkOnClose = false
        )
      )
      _ <- ZIO.acquireRelease(ZIO.succeed(database)) { db =>
        ZIO.foreachParDiscard(dbMaterialization.columnFamilySet.value) { column => db.dropColumnFamily(column).orDie }
      }
    } yield database.asInstanceOf[FdbDatabase[TestDatabase.CfSet]]
  }
}

final class FdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat: TestDatabase.Materialization = FdbDatabaseTest.dbMaterialization
  protected val managedDb = FdbDatabaseTest.managedDb
}
