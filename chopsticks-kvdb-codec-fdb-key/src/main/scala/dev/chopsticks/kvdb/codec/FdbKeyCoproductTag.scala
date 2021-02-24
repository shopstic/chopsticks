package dev.chopsticks.kvdb.codec

import scala.annotation.implicitNotFound

@implicitNotFound(
  msg =
    "Implicit FdbKeyCoproductTag[${A}] not found. Try supplying an implicit instance of FdbKeyCoproductTag[${A}]"
)
trait FdbKeyCoproductTag[A] {
  type Tag

  def tagSerializer: FdbKeySerializer[Tag]
  def tagDeserializer: FdbKeyDeserializer[Tag]

  def typeNameToTag(shortTypeName: String): Tag
  def valueToTag(value: A): Tag
}

object FdbKeyCoproductTag {
  type Aux[A, T] = FdbKeyCoproductTag[A] {
    type Tag = T
  }

  def apply[A, T: FdbKeySerializer: FdbKeyDeserializer](simpleTypeToTag: String => T): FdbKeyCoproductTag.Aux[A, T] = {
    new FdbKeyCoproductTag[A] {
      type Tag = T

      def tagSerializer: FdbKeySerializer[Tag] = implicitly[FdbKeySerializer[T]]
      def tagDeserializer: FdbKeyDeserializer[Tag] = implicitly[FdbKeyDeserializer[T]]

      override def typeNameToTag(shortTypeName: String): Tag =
        simpleTypeToTag(shortTypeName)

      override def valueToTag(value: A): Tag =
        typeNameToTag(value.getClass.getSimpleName)

    }
  }

}
