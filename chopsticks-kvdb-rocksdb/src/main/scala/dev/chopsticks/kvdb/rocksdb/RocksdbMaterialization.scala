package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.KvdbMaterialization.DuplicatedColumnFamilyIdsException

import scala.language.higherKinds

trait RocksdbMaterialization[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] {
  def defaultColumnFamily: BCF[_, _]
  def columnFamilyConfigMap: RocksdbColumnFamilyConfigMap[BCF, CFS]
}

object RocksdbMaterialization {
  def validate[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    mat: RocksdbMaterialization[BCF, CFS]
  ): Either[DuplicatedColumnFamilyIdsException, RocksdbMaterialization[BCF, CFS]] = {
    val dups = mat.columnFamilyConfigMap.map.keys
      .groupBy { cf =>
        if (cf == mat.defaultColumnFamily) RocksdbDatabase.DEFAULT_COLUMN_NAME else cf.id
      }
      .filter(_._2.size > 1)

    if (dups.nonEmpty) {
      Left(DuplicatedColumnFamilyIdsException(s"Duplicated ids found for the following column families: ${dups
        .map {
          case (id, set) =>
            s"$id -> ${set.map(_.getClass.getName).mkString(", ")}"
        }
        .mkString(" and ")}"))
    }
    else Right(mat)
  }
}
