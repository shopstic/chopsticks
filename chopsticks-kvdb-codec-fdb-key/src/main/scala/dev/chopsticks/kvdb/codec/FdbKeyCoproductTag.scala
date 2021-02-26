package dev.chopsticks.kvdb.codec

import magnolia.{Subtype, TypeName}

import scala.annotation.implicitNotFound

@implicitNotFound(
  msg =
    "Implicit FdbKeyCoproductTag[${A}] not found. Try supplying an implicit instance of FdbKeyCoproductTag[${A}]"
)
trait FdbKeyCoproductTag[A] {
  type Tag
  def subTypeToTag[F[_]](subType: Subtype[F, A]): Tag
  def tagToSubType[F[_]](allSubTypes: Seq[Subtype[F, A]], tag: Tag): Option[Subtype[F, A]]
}

object FdbKeyCoproductTag {
  type Aux[A, T] = FdbKeyCoproductTag[A] {
    type Tag = T
  }

  def apply[A, T](typeNameToTag: TypeName => T): FdbKeyCoproductTag.Aux[A, T] = {
    new FdbKeyCoproductTag[A] {
      type Tag = T
      override def subTypeToTag[F[_]](subType: Subtype[F, A]): Tag = typeNameToTag(subType.typeName)
      override def tagToSubType[F[_]](allSubTypes: Seq[Subtype[F, A]], tag: Tag): Option[Subtype[F, A]] = {
        allSubTypes.find(subType => subTypeToTag(subType) == tag)
      }
    }
  }
}
