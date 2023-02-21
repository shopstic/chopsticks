package dev.chopsticks.sample.kvdb

import java.time.Instant
import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.codec.{FdbKeyDeserializer, FdbKeySerializer, ValueSerdes}
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbMaterialization
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}

object SampleDb extends KvdbDefinition:
  final case class TestKeyWithVersionstamp(foo: String, bar: Int, version: Versionstamp) derives FdbKeySerializer,
        FdbKeyDeserializer
  final case class TestValueWithVersionstamp(version: Versionstamp) derives FdbKeySerializer, FdbKeyDeserializer

  import dev.chopsticks.kvdb.codec.primitive.*
  import dev.chopsticks.kvdb.codec.FdbKeySerializer.*
  import dev.chopsticks.kvdb.codec.FdbKeyDeserializer.*

  import dev.chopsticks.kvdb.codec.primitive.literalStringDbValue

  implicit val testVersionstampValueSerdes: ValueSerdes[TestValueWithVersionstamp] =
    ValueSerdes.fromKeySerdes[TestValueWithVersionstamp]
  implicit val literalVersionstampValueSerdes: ValueSerdes[Versionstamp] =
    ValueSerdes.fromKeySerdes[Versionstamp]

  abstract class Default extends ColumnFamily[String, String]("Default")
  abstract class VersionstampKeyTest extends ColumnFamily[TestKeyWithVersionstamp, String]("VersionstampKeyTest")
  abstract class Time extends ColumnFamily[Instant, String]("Time")
  abstract class VersionstampValueTest extends ColumnFamily[String, TestValueWithVersionstamp]("VersionstampValueTest")
  abstract class LiteralVersionstampValueTest extends ColumnFamily[String, Versionstamp]("LiteralVersionstampValueTest")

  type CfSet =
    Default &
      VersionstampKeyTest &
      Time &
      VersionstampValueTest &
      LiteralVersionstampValueTest

  trait Materialization extends KvdbMaterialization[CfSet] with FdbMaterialization:
    def default: Default
    def versionstampKeyTest: VersionstampKeyTest
    def time: Time
    def versionstampValueTest: VersionstampValueTest
    def literalVersionstampValueTest: LiteralVersionstampValueTest

    override lazy val columnFamilySet: ColumnFamilySet[CfSet] = {
      ColumnFamilySet
        .of(default)
        .and(versionstampKeyTest)
        .and(time)
        .and(versionstampValueTest)
        .and(literalVersionstampValueTest)
    }
  end Materialization
end SampleDb
