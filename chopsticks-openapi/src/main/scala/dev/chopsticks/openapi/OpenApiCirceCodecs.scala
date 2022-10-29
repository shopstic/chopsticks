package dev.chopsticks.openapi

import cats.data.{NonEmptyList, Validated}
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import zio.Chunk

import scala.collection.mutable

object OpenApiCirceCodecs {
  implicit final def decodeChunk[A](implicit decodeA: Decoder[A]): Decoder[Chunk[A]] =
    new OpenApiCirceSeqDecoder[A, Chunk](decodeA) {
      final protected def createBuilder(): mutable.Builder[A, Chunk[A]] = zio.ChunkBuilder.make[A]()
    }
  implicit final def encodeChunk[A](implicit encodeA: Encoder[A]): Encoder[Chunk[A]] =
    new OpenApiCirceIterableAsArrayEncoder[A, Chunk](encodeA) {
      override protected def toIterator(a: Chunk[A]): Iterator[A] = a.iterator
    }

}

// impl from circe
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class OpenApiCirceSeqDecoder[A, C[_]](decodeA: Decoder[A]) extends Decoder[C[A]] {
  protected def createBuilder(): mutable.Builder[A, C[A]]

  def apply(c: HCursor): Decoder.Result[C[A]] = {
    var current = c.downArray

    if (current.succeeded) {
      val builder = createBuilder()
      var failed: DecodingFailure = null

      while (failed.eq(null) && current.succeeded) {
        decodeA(current.asInstanceOf[HCursor]) match {
          case Left(e) => failed = e
          case Right(a) =>
            builder += a
            current = current.right
        }
      }

      if (failed.eq(null)) Right(builder.result()) else Left(failed)
    }
    else {
      if (c.value.isArray) Right(createBuilder().result())
      else {
        Left(DecodingFailure("C[A]", c.history))
      }
    }
  }

  override def decodeAccumulating(c: HCursor): Decoder.AccumulatingResult[C[A]] = {
    var current = c.downArray

    if (current.succeeded) {
      val builder = createBuilder()
      var failed = false
      val failures = List.newBuilder[DecodingFailure]

      while (current.succeeded) {
        decodeA.decodeAccumulating(current.asInstanceOf[HCursor]) match {
          case Validated.Invalid(es) =>
            failed = true
            failures += es.head
            failures ++= es.tail
          case Validated.Valid(a) =>
            if (!failed) builder += a
        }
        current = current.right
      }

      if (!failed) Validated.valid(builder.result())
      else {
        failures.result() match {
          case h :: t => Validated.invalid(NonEmptyList(h, t))
          case Nil => Validated.valid(builder.result())
        }
      }
    }
    else {
      if (c.value.isArray) Validated.valid(createBuilder().result())
      else {
        Validated.invalidNel(DecodingFailure("C[A]", c.history))
      }
    }
  }
}

// impl from circe
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract protected[this] class OpenApiCirceIterableAsArrayEncoder[A, C[_]](encodeA: Encoder[A])
    extends Encoder.AsArray[C[A]] {
  protected def toIterator(a: C[A]): Iterator[A]

  final def encodeArray(a: C[A]): Vector[Json] = {
    val builder = Vector.newBuilder[Json]
    val iterator = toIterator(a)

    while (iterator.hasNext) {
      builder += encodeA(iterator.next())
    }

    builder.result()
  }
}
