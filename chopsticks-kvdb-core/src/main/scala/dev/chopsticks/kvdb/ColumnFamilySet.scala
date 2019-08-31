package dev.chopsticks.kvdb

object ColumnFamilySet {
  def apply[CF <: ColumnFamily[_, _]] = new ColumnFamilySet[CF, CF](Set.empty)
}

final class ColumnFamilySet[CF <: ColumnFamily[_, _], +A <: CF] private (val set: Set[CF]) {
  def and[B <: CF](cf: B): ColumnFamilySet[CF, A with B] = {
    new ColumnFamilySet[CF, A with B](set + cf)
  }

  def of[B <: CF](cf: B): ColumnFamilySet[CF, B] = {
    new ColumnFamilySet[CF, B](Set(cf))
  }
}
