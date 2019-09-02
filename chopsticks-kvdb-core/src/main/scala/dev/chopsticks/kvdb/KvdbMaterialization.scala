package dev.chopsticks.kvdb

import scala.language.higherKinds

trait KvdbMaterialization[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] {
  def columnFamilySet: ColumnFamilySet[BCF, CF]
}
