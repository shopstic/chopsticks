package dev.chopsticks.kvdb.codec

import com.apple.foundationdb.tuple.Tuple
import magnolia.{CaseClass, Magnolia}
import shapeless.{HList, HNil, :: => :::}

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.reflect.ClassTag

@implicitNotFound(
  msg =
    "Implicit FdbKeyPrefixSerializer[${T}] not found. Try supplying an implicit instance of FdbKeyPrefixSerializer[${T}]"
)
trait FdbKeyPrefixSerializer[T] {
  def serializePrefix(tupleOutput: Tuple, prefix: HList): (Tuple, HList)
}

object FdbKeyPrefixSerializer {
  type Typeclass[A] = FdbKeyPrefixSerializer[A]

  implicit def predefinedKeyPrefixSerializer[A](implicit
    serializer: PredefinedFdbKeySerializer[A],
    ct: ClassTag[A]
  ): FdbKeyPrefixSerializer[A] = {
    (tupleOutput: Tuple, prefix: HList) =>
      {
        prefix match {
          case (head: A) ::: tail =>
            serializer.serialize(tupleOutput, head) -> tail
          case _ =>
            throw new IllegalStateException(s"Invalid prefix: $prefix for ${ct.toString()}")
        }
      }
  }

  implicit def sealedTraitKeyPrefixSerializer[A](implicit
    serializer: SealedTraitFdbKeySerializer[A],
    ct: ClassTag[A]
  ): FdbKeyPrefixSerializer[A] = {
    (tupleOutput: Tuple, prefix: HList) =>
      {
        prefix match {
          case (head: A) ::: tail =>
            serializer.serialize(tupleOutput, head) -> tail
          case _ =>
            throw new IllegalStateException(s"Invalid prefix: $prefix for ${ct.toString()}")
        }
      }
  }

  def combine[T](ctx: CaseClass[FdbKeyPrefixSerializer, T]): FdbKeyPrefixSerializer[T] =
    (tupleOutput: Tuple, prefix: HList) => {
      // Deliberately optimized for simplicity & best performance
      var result = (tupleOutput, prefix)
      var done = false
      val params = ctx.parameters.iterator

      while (!done && params.hasNext) {
        result match {
          case (output, remaining) =>
            if (remaining == HNil) {
              done = true
            }
            else {
              println(s"${ctx.typeName.full} remaining = $remaining")
              result = params.next().typeclass.serializePrefix(output, remaining)
            }
        }
      }

      result
    }

  implicit def derivePrefixSerializer[A]: FdbKeyPrefixSerializer[A] = macro Magnolia.gen[A]

  def apply[V](implicit f: FdbKeyPrefixSerializer[V]): FdbKeyPrefixSerializer[V] = f
}
