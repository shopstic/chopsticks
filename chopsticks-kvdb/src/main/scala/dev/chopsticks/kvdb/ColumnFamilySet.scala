package dev.chopsticks.kvdb

object ColumnFamilySet:
  def of[CF <: ColumnFamily[_, _]](cf: CF): ColumnFamilySet[CF] =
    new ColumnFamilySet(Set(cf))

final class ColumnFamilySet[CF <: ColumnFamily[_, _]] private (val value: Set[ColumnFamily[_, _]]):
  def and[CF2 <: ColumnFamily[_, _]](cf: CF2): ColumnFamilySet[CF & CF2] =
    new ColumnFamilySet[CF & CF2](value + cf)

  def filter(f: ColumnFamily[_, _] => Boolean): ColumnFamilySet[CF] =
    new ColumnFamilySet[CF](value.filter(f))
