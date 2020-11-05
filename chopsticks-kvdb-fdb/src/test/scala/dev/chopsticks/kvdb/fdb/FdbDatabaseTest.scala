package dev.chopsticks.kvdb.fdb

import java.util.UUID

import dev.chopsticks.fp.AkkaDiApp
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, LookupCf, PlainCf}
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import zio.{ZIO, ZManaged}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.util.KvdbIoThreadPool

object FdbDatabaseTest {
  object dbMaterialization extends TestDatabase.Materialization with FdbMaterialization[TestDatabase.BaseCf] {
    object plain extends PlainCf
    object lookup extends LookupCf
    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of plain and lookup
    }
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampKey = Set.empty
    //noinspection TypeAnnotation
    override val keyspacesWithVersionstampValue = Set.empty
  }

  val managedDb: ZManaged[AkkaDiApp.Env with KvdbIoThreadPool with IzLogging, Throwable, TestDatabase.Db] = {
    for {
      database <- FdbDatabase.manage(
        dbMaterialization,
        FdbDatabase.FdbDatabaseConfig(
          clusterFilePath = Some(sys.env("HOME") + "/.fdb/cluster.file"),
          rootDirectoryPath = UUID.randomUUID().toString,
          stopNetworkOnClose = false
        )
      )
      _ <- ZManaged.make(ZIO.succeed(database)) { db =>
        ZIO.foreachPar_(dbMaterialization.columnFamilySet.value) { column => db.dropColumnFamily(column).orDie }
      }
    } yield database
  }
}

final class FdbDatabaseTest extends KvdbDatabaseTest {
  protected val dbMat = FdbDatabaseTest.dbMaterialization
  protected val managedDb = FdbDatabaseTest.managedDb
}
