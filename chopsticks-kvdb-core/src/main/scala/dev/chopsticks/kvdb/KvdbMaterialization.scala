package dev.chopsticks.kvdb

trait KvdbMaterialization[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] {
  def columnFamilySet: ColumnFamilySet[BCF, CF]
}

object KvdbMaterialization {
  final case class DuplicatedColumnFamilyIdsException(message: String) extends IllegalArgumentException(message)

  def validate[BCF[A, B] <: ColumnFamily[A, B], CF <: BCF[_, _]](
    mat: KvdbMaterialization[BCF, CF]
  ): Either[DuplicatedColumnFamilyIdsException, KvdbMaterialization[BCF, CF]] = {
    val dups = mat.columnFamilySet.value.groupBy(_.id).filter(_._2.size > 1)

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
