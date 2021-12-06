package dev.chopsticks.kvdb.codec

import com.sleepycat.bind.tuple.TupleOutput
import magnolia.{CaseClass, Magnolia}
import shapeless.{:: => :::, HList, HNil}

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.reflect.ClassTag

@implicitNotFound(
  msg =
    "Implicit BerkeleydbKeyPrefixSerializer[${T}] not found. Try supplying an implicit instance of BerkeleydbKeyPrefixSerializer[${T}]"
)
trait BerkeleydbKeyPrefixSerializer[T] {
  def serializePrefix(tupleOutput: TupleOutput, prefix: HList): (TupleOutput, HList)
}

object BerkeleydbKeyPrefixSerializer {
  type Typeclass[A] = BerkeleydbKeyPrefixSerializer[A]

  implicit def predefinedKeyPrefixSerializer[A](implicit
    serializer: PredefinedBerkeleydbKeySerializer[A],
    ct: ClassTag[A]
  ): BerkeleydbKeyPrefixSerializer[A] = {
    (tupleOutput: TupleOutput, prefix: HList) =>
      {
        prefix match {
          case (head: A) ::: tail =>
            serializer.serialize(tupleOutput, head) -> tail
          case _ =>
            throw new IllegalStateException(s"Invalid prefix: $prefix for ${ct.toString()}")
        }
      }
  }

  def combine[T](ctx: CaseClass[BerkeleydbKeyPrefixSerializer, T]): BerkeleydbKeyPrefixSerializer[T] =
    (tupleOutput: TupleOutput, prefix: HList) => {
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
              result = params.next().typeclass.serializePrefix(output, remaining)
            }
        }
      }

      result
    }

  implicit def derivePrefixSerializer[A]: BerkeleydbKeyPrefixSerializer[A] = macro Magnolia.gen[A]

  def apply[V](implicit f: BerkeleydbKeyPrefixSerializer[V]): BerkeleydbKeyPrefixSerializer[V] = f
}
