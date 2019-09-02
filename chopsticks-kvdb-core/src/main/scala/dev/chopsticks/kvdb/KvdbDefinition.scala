package dev.chopsticks.kvdb

trait KvdbDefinition {
  trait BaseCf[K, V] extends ColumnFamily[K, V]

  type CfSet <: BaseCf[_, _]
  type Db = KvdbDatabase[BaseCf, CfSet]

  type Materialization <: KvdbMaterialization[BaseCf, CfSet]
}
