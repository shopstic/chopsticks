package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.DbInterface.{DbColumn, DbColumns, DbDefinitionOf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.codec.{DbKey, DbValue}
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions

sealed abstract class TestDbColumn[K: DbKey, V: DbValue] extends DbColumn[K, V]

object TestDbColumns extends DbColumns[TestDbColumn[_, _]] {

  import squants.information.InformationConversions._

  //noinspection TypeAnnotation
  val values = findValues

  case object Default extends TestDbColumn[String, String] {
    val rocksdbOptions: RocksdbCFOptions = RocksdbCFOptions(64.mib, 64.mib, 1)
  }

  case object Lookup extends TestDbColumn[String, String] {
    val rocksdbOptions: RocksdbCFOptions = RocksdbCFOptions(64.mib, 64.mib, 0)
  }

//  case object Checkpoint extends TestDbColumn[String, LocalDateTime] {
//    val rocksdbOptions: RocksdbCFOptions = RocksdbCFOptions(64.mib, 64.mib, 0)
//  }

}

object TestDb extends DbDefinitionOf[TestDbColumn, TestDbColumns.type](TestDbColumns)
