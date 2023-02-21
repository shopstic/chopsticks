package dev.chopsticks.kvdb

trait KvdbMaterialization[CFS <: ColumnFamily[_, _]]:
  def columnFamilySet: ColumnFamilySet[CFS]

object KvdbMaterialization:
  final case class DuplicatedColumnFamilyIdsException(message: String) extends IllegalArgumentException(message)

  def validate[CFS <: ColumnFamily[_, _]](
    mat: KvdbMaterialization[CFS]
  ): Either[DuplicatedColumnFamilyIdsException, KvdbMaterialization[CFS]] =
    val dups = mat.columnFamilySet.value.groupBy(_.id).filter(_._2.size > 1)
    if (dups.nonEmpty)
      Left(DuplicatedColumnFamilyIdsException(s"Duplicated ids found for the following column families: ${dups
        .map {
          case (id, set) =>
            s"$id -> ${set.map(_.getClass.getName).mkString(", ")}"
        }
        .mkString(" and ")}"))
    else
      Right(mat)
