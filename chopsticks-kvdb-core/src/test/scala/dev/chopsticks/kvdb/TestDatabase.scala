package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi

import java.time.YearMonth

object TestDatabase extends KvdbDefinition {
  type TestDbCf[K, V] = BaseCf[K, V]

  trait PlainCf extends TestDbCf[String, String]
  trait LookupCf extends TestDbCf[String, String]
  trait CounterCf extends TestDbCf[String, Long]
  trait MinCf extends TestDbCf[String, YearMonth]
  trait MaxCf extends TestDbCf[String, YearMonth]

  sealed trait AnotherDbCf[K, V] extends ColumnFamily[K, V]
  trait AnotherCf1 extends AnotherDbCf[String, String]
  trait AnotherCf2 extends AnotherDbCf[String, Int]

  type CfSet = PlainCf with LookupCf with CounterCf with MinCf with MaxCf

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
    def plain: PlainCf
    def lookup: LookupCf
    def counter: CounterCf
    def min: MinCf
    def max: MaxCf
  }

  type DbApi = KvdbDatabaseApi[TestDbCf]
}
