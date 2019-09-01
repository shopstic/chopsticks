package dev.chopsticks.kvdb

object TestDatabase {
  sealed trait TestCf[K, V] extends ColumnFamily[K, V]
  trait DefaultCf extends TestCf[String, String]
  trait LookupCf extends TestCf[String, String]

  trait AnotherCf1 extends ColumnFamily[String, String]
  trait AnotherCf2 extends ColumnFamily[String, Int]
  type TestDb = KvdbDatabase[TestCf, DefaultCf with LookupCf]
}
