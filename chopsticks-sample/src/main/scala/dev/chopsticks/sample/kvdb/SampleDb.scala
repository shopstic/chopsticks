package dev.chopsticks.sample.kvdb

import java.time.Instant

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.fdb.FdbMaterialization
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}

object SampleDb extends KvdbDefinition {
  final case class TestKeyWithVersionstamp(foo: String, bar: Int, version: Versionstamp)
  final case class TestValueWithVersionstamp(version: Versionstamp)

  trait Default extends BaseCf[String, String]
  trait VersionstampKeyTest extends BaseCf[TestKeyWithVersionstamp, String]
  trait Time extends BaseCf[Instant, String]
  trait VersionstampValueTest extends BaseCf[String, TestValueWithVersionstamp]

  type CfSet = Default with VersionstampKeyTest with Time with VersionstampValueTest

  trait Materialization extends KvdbMaterialization[BaseCf, CfSet] with FdbMaterialization[BaseCf] {
    def default: Default
    def versionstampKeyTest: VersionstampKeyTest
    def time: Time
    def versionstampValueTest: VersionstampValueTest

    override lazy val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = {
      ColumnFamilySet[BaseCf]
        .of(default)
        .and(versionstampKeyTest)
        .and(time)
        .and(versionstampValueTest)
    }
  }
}
