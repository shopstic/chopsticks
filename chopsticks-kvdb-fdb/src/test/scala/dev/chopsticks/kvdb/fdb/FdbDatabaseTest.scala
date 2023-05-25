package dev.chopsticks.kvdb.fdb

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.kvdb.TestDatabase.{BaseCf, CfSet, CounterCf, LookupCf, PlainCf}
import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest, TestDatabase}
import zio.{RManaged, ZIO, ZManaged}
import dev.chopsticks.kvdb.codec.primitive._

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

object FdbDatabaseTest {
  implicit val littleIndianLongValueSerdes: ValueSerdes[Long] =
    ValueSerdes.create[Long](
      value => ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array(),
      bytes => Right(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong())
    )

  object dbMaterialization extends TestDatabase.Materialization with FdbMaterialization[TestDatabase.BaseCf] {
    object plain extends PlainCf
    object lookup extends LookupCf
    object counter extends CounterCf

    val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf].of(plain).and(lookup).and(counter)
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
