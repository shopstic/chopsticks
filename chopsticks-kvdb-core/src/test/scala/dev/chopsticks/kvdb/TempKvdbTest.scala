package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.TestDatabase.{AnotherCf1, AnotherCf2, DefaultCf, LookupCf, TestDb, TestDbCf}
import zio.Task

object TempKvdbTest {
  private def populateColumn[CF <: TestDbCf[K, V], K, V](
    db: TestDb,
    column: CF,
    pairs: Seq[(K, V)]
  ): Task[Unit] = {
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)
  }
}

abstract class TempKvdbTest {
  def defaultCf: DefaultCf
  def lookupCf: LookupCf
  def anotherCf1: AnotherCf1
  def anotherCf2: AnotherCf2
  def db: TestDb

//  implicitly[D <:< TestCf[_, _]]
  TempKvdbTest.populateColumn(db, defaultCf, List("foo" -> "bar"))
  TempKvdbTest.populateColumn(db, lookupCf, List("foo" -> "bar"))
//  TempKvdbTest.populateColumn(db, anotherCf1, List("foo" -> "bar"))
//  TempKvdbTest.populateColumn(db, anotherCf2, List("foo" -> 1))
}
