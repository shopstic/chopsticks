package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi

object TestDatabase {
  sealed trait TestDbCf[K, V] extends ColumnFamily[K, V]
  trait DefaultCf extends TestDbCf[String, String]
  trait LookupCf extends TestDbCf[String, String]

  sealed trait AnotherDbCf[K, V] extends ColumnFamily[K, V]
  trait AnotherCf1 extends AnotherDbCf[String, String]
  trait AnotherCf2 extends AnotherDbCf[String, Int]

  type TestDb = KvdbDatabase[TestDbCf, DefaultCf with LookupCf]
  type TestDbApi = KvdbDatabaseApi[TestDbCf]
}
