package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.primitive.*
import dev.chopsticks.kvdb.codec.ValueSerdes

import java.nio.{ByteBuffer, ByteOrder}

object TestDatabase extends KvdbDefinition {
//  type TestDbCf[K, V] = BaseCf[K, V]
  implicit val littleIndianLongValueSerdes: ValueSerdes[Long] =
    ValueSerdes.create[Long](
      value => ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array(),
      bytes => Right(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong())
    )

  abstract class PlainCf extends ColumnFamily[String, String]("plainCf")
  abstract class LookupCf extends ColumnFamily[String, String]("lookupCf")
  abstract class CounterCf extends ColumnFamily[String, Long]("counterCf")

  type CfSet = PlainCf with LookupCf with CounterCf

  trait Materialization extends KvdbMaterialization[CfSet] {
    def plain: PlainCf
    def lookup: LookupCf
    def counter: CounterCf
  }

  type DbApi = KvdbDatabaseApi[CfSet]
}
