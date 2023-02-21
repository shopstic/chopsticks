package dev.chopsticks.kvdb

trait KvdbDefinition:
  type CfSet <: ColumnFamily[_, _]
  type Db = KvdbDatabase[CfSet]

  type Materialization <: KvdbMaterialization[CfSet]
end KvdbDefinition
