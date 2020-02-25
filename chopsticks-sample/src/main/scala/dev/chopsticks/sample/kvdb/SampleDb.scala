package dev.chopsticks.sample.kvdb

import java.time.Instant

import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import zio.Has

object SampleDb extends KvdbDefinition {
  trait Default extends BaseCf[String, String]
  trait Time extends BaseCf[Instant, String]

  type CfSet = Default with Time

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
    def default: Default
    def time: Time
    lazy val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of default and time
    }
  }

  type Env = Has[Db]
}
