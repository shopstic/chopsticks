package dev.chopsticks.sample.kvdb

import java.time.Instant

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.fdb.FdbMaterialization
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import zio.Has

object SampleDb extends KvdbDefinition {
  final case class TestKey(foo: String, bar: Int, version: Versionstamp)

  trait Default extends BaseCf[String, String]
  trait Test extends BaseCf[TestKey, String]
  trait Time extends BaseCf[Instant, String]

  type CfSet = Default with Test with Time

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] with FdbMaterialization[BaseCf] {
    def default: Default
    def test: Test
    def time: Time
    override lazy val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf]
        .of(default)
        .and(test)
        .and(time)
    }
  }

  type Env = Has[Db]
}
