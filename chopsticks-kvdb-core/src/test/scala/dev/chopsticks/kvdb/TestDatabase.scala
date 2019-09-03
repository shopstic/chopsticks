package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi

object TestDatabase extends KvdbDefinition {
  type TestDbCf[K, V] = BaseCf[K, V]

  trait PlainCf extends TestDbCf[String, String]
  trait LookupCf extends TestDbCf[String, String]

  sealed trait AnotherDbCf[K, V] extends ColumnFamily[K, V]
  trait AnotherCf1 extends AnotherDbCf[String, String]
  trait AnotherCf2 extends AnotherDbCf[String, Int]

  type CfSet = PlainCf with LookupCf

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
    def plain: PlainCf
    def lookup: LookupCf
  }

  type DbApi = KvdbDatabaseApi[TestDbCf]
}
