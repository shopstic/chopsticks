package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi

object TestDatabase extends KvdbDefinition {
  type TestDbCf[K, V] = BaseCf[K, V]

  trait PlainCf extends TestDbCf[String, String]
  trait LookupCf extends TestDbCf[String, String]
  trait CounterCf extends TestDbCf[String, Long]

  sealed trait AnotherDbCf[K, V] extends ColumnFamily[K, V]
  trait AnotherCf1 extends AnotherDbCf[String, String]
  trait AnotherCf2 extends AnotherDbCf[String, Int]

  type CfSet = PlainCf with LookupCf with CounterCf

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
    def plain: PlainCf
    def lookup: LookupCf
    def counter: CounterCf
  }

  type DbApi = KvdbDatabaseApi[TestDbCf]
}
