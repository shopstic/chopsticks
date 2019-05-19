package dev.chopsticks.util

import org.rocksdb._
import squants.information.Information
import squants.information.InformationConversions._

import scala.language.implicitConversions

object RocksdbPersistentMap {
  implicit private def informationToLong(i: Information): Long = i.toBytes.toLong

  val defaultOptions: Options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(2.mib)
    .setTargetFileSizeBase(2.mib)
    .setMaxBytesForLevelBase(10.mib)
    .setSoftPendingCompactionBytesLimit(256.mib)
    .setHardPendingCompactionBytesLimit(1.gib)
    .optimizeForPointLookup(8)

  def apply[V](
    db: RocksDB,
    column: ColumnFamilyHandle
  )(
    implicit ser: V => Array[Byte],
    des: Array[Byte] => V,
    readOptions: ReadOptions,
    writeOptions: WriteOptions
  ): RocksdbPersistentMap[V] = {
    new RocksdbPersistentMap[V](db, column)
  }
}

class RocksdbPersistentMap[V](db: RocksDB, column: ColumnFamilyHandle)(
  implicit ser: V => Array[Byte],
  des: Array[Byte] => V,
  readOptions: ReadOptions,
  writeOptions: WriteOptions
) {

  private val UTF8 = "UTF-8"

  def put(key: String, value: V): Unit = {
    db.put(column, writeOptions, key.getBytes(UTF8), ser(value))
  }

  def mergeBatch(batch: List[(String, V)]): Unit = {
    val writeBatch = new WriteBatch()
    batch.foreach {
      case (key, value) =>
        writeBatch.merge(column, key.getBytes(UTF8), ser(value))
    }
    db.write(writeOptions, writeBatch)
  }

  def merge(key: String, value: V): Unit = {
    db.merge(column, writeOptions, key.getBytes(UTF8), ser(value))
  }

  def get(key: String): Option[V] = {
    val bytes = Option(db.get(column, readOptions, key.getBytes(UTF8)))
    bytes.map(b => des(b))
  }

  def close(): Unit = {
    db.close()
  }
}
