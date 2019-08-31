package dev.chopsticks.kvdb_new
import enumeratum.{Enum, _}

import scala.language.higherKinds

object KvdbNewTest {

  trait KvdbColumnFamily[K, V] extends EnumEntry

  class KvdbColumnFamilyApi[CF[A, B] <: KvdbColumnFamily[A, B], K, V](cf: CF[K, V]) {
    def get(key: K): V = ???
    def put(key: K, value: V): Unit = ???
    def iterate: Iterator[(K, V)] = ???
  }

  trait KvdbDatabase[BCF <: KvdbColumnFamily[_, _]] {
    def cf[CF[A, B] <: KvdbColumnFamily[A, B], K, V](cf: CF[K, V])(implicit ev: CF[K, V] <:< BCF): KvdbColumnFamilyApi[CF, K, V] =
      new KvdbColumnFamilyApi[CF, K, V](cf)
  }

  sealed trait TestDbKvdbColumnFamily[K, V] extends KvdbColumnFamily[K, V]

  object TestDbKvdbColumnFamilies extends enumeratum.Enum[TestDbKvdbColumnFamily[_, _]] {
    case object Foo extends TestDbKvdbColumnFamily[String, Int]
    case object Bar extends TestDbKvdbColumnFamily[Boolean, Long] {}
    //noinspection TypeAnnotation
    val values = findValues
  }

  case object Baz extends KvdbColumnFamily[Double, Int] {}

//  trait Foo extends KvdbColumnFamily[String, Int]
//  object Foo extends Foo {
//    val id = "foo"
//  }
//  trait Bar extends KvdbColumnFamily[Boolean, Long]
//  object Bar extends Bar {
//    val id = "bar"
//  }
//
//  trait Baz extends KvdbColumnFamily[Double, String]
//  object Baz extends Baz {
//    val id = "baz"
//  }

  abstract class TestDb extends KvdbDatabase[TestDbKvdbColumnFamily[_, _]]

  def doStuff(db: TestDb) = {
    db.cf(TestDbKvdbColumnFamilies.Foo).put("test", 1)
  }
  object TestDb extends TestDb {
    val columns = TestDbKvdbColumnFamilies
  }
//
//  val _ = TestDb.cf(TestDbKvdbColumnFamilies.Foo).get("test")
//  val _ = TestDb.cf(TestDbKvdbColumnFamilies.Foo).get("test")
  doStuff(TestDb)
//  TestDb.cf(Bar).get(true)

}
