package dev.chopsticks.sample.kvdb

import dev.chopsticks.kvdb.DbClient
import dev.chopsticks.kvdb.DbInterface.{DbColumn, DbColumns, DbDefinitionOf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.codec.{DbKey, DbValue}
import dev.chopsticks.kvdb.util.RocksdbCFBuilder
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.{PointLookupPattern, PrefixedScanPattern, RocksdbCFOptions}
import eu.timepit.refined.auto._

sealed abstract class DummyTestKvdbColumn[K: DbKey, V: DbValue] extends DbColumn[K, V]

object DummyTestKvdbColumns extends DbColumns[DummyTestKvdbColumn[_, _]] {

  import squants.information.InformationConversions._

  //noinspection TypeAnnotation
  val values = findValues

  case object Default extends DummyTestKvdbColumn[String, String] {
    val rocksdbOptions: RocksdbCFOptions =
      RocksdbCFBuilder.RocksdbCFOptions(
        memoryBudget = 64.kib,
        blockCache = 64.kib,
        blockSize = 16.kib,
        PrefixedScanPattern(4)
      )
  }

  case object Lookup extends DummyTestKvdbColumn[String, String] {
    val rocksdbOptions: RocksdbCFOptions =
      RocksdbCFBuilder.RocksdbCFOptions(
        memoryBudget = 64.kib,
        blockCache = 64.kib,
        blockSize = 16.kib,
        PointLookupPattern
      )
  }
}

object DummyTestKvdb extends DbDefinitionOf[DummyTestKvdbColumn, DummyTestKvdbColumns.type](DummyTestKvdbColumns)

trait DummyTestKvdbEnv {
  def dummyTestKvdb: DbClient[DummyTestKvdb.type]
}
