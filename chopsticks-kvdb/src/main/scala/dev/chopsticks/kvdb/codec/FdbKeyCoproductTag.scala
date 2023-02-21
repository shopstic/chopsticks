package dev.chopsticks.kvdb.codec

import magnolia1.*
import scala.annotation.implicitNotFound

@implicitNotFound(
  msg =
    "Implicit FdbKeyCoproductTag[${A}] not found. Try supplying an implicit instance of FdbKeyCoproductTag[${A}]"
)
trait FdbKeyCoproductTag[A]:
  type Tag
  def subTypeToTag[F[_]](subType: SealedTrait.SubtypeValue[F, A, _]): Tag
  def tagToSubType[F[_]](
    allSubTypes: Seq[SealedTrait.SubtypeValue[F, A, _]],
    tag: Tag
  ): Option[SealedTrait.SubtypeValue[F, A, _]]

object FdbKeyCoproductTag:
  type Aux[A, T] = FdbKeyCoproductTag[A] {
    type Tag = T
  }

  def apply[A, T](typeNameToTag: TypeInfo => T): FdbKeyCoproductTag.Aux[A, T] =
    new FdbKeyCoproductTag[A]:
      type Tag = T
      override def subTypeToTag[F[_]](subType: SealedTrait.SubtypeValue[F, A, _]): Tag =
        typeNameToTag(subType.subtype.typeInfo)
      override def tagToSubType[F[_]](
        allSubTypes: Seq[SealedTrait.SubtypeValue[F, A, _]],
        tag: Tag
      ): Option[SealedTrait.SubtypeValue[F, A, _]] =
        allSubTypes.find(subType => subTypeToTag(subType) == tag)
