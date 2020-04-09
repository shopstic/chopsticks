package dev.chopsticks.sample.kvdb

import java.time.Instant

import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import zio.Has

object SampleDb extends KvdbDefinition {
  final case class TestKey(foo: String, bar: Int)

  trait Default extends BaseCf[String, String]
  trait Test extends BaseCf[TestKey, String]
  trait Time extends BaseCf[Instant, String]

  type CfSet = Default with Test with Time

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
    def default: Default
    def test: Test
    def time: Time
    lazy val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf] of default and test and time
    }
  }

  type Env = Has[Db]
}
