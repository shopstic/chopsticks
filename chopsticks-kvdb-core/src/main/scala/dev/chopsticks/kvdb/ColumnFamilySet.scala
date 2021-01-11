package dev.chopsticks.kvdb

object ColumnFamilySet {
  def apply[CF[A, B] <: ColumnFamily[A, B]] = new ColumnFamilySet[CF, CF[_, _]](Set.empty)
}

final class ColumnFamilySet[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] private (val value: Set[BCF[_, _]]) {
  def and[B <: BCF[_, _]](cf: B): ColumnFamilySet[BCF, CF with B] = {
    new ColumnFamilySet[BCF, CF with B](value + cf)
  }

  def of[B <: BCF[_, _]](cf: B): ColumnFamilySet[BCF, B] = {
    new ColumnFamilySet[BCF, B](Set(cf))
  }

  def filter(fn: BCF[_, _] => Boolean): ColumnFamilySet[BCF, CF] = {
    new ColumnFamilySet[BCF, CF](value.filter(fn))
  }
}
