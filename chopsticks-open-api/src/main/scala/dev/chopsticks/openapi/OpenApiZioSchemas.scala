package dev.chopsticks.openapi

import cats.data.NonEmptyList
import eu.timepit.refined.types.all.NonNegInt
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import sttp.tapir.Validator
import zio.schema.Schema
import zio.schema.internal.SourceLocation
import zio.Chunk
import zio.schema.StandardType.InstantType

import java.time.format.DateTimeFormatter

object OpenApiZioSchemas {
  implicit class ZioSchemaOps[A](val schema: Schema[A]) extends AnyVal {
    def named(name: String): Schema[A] = {
      schema.annotate(OpenApiAnnotations.entityName(name))
    }
    def validate(validator: Validator[A]): Schema[A] = {
      schema.annotate(new sttp.tapir.Schema.annotations.validate[A](validator))
    }
    def description(description: String): Schema[A] = {
      schema.annotate(new sttp.tapir.Schema.annotations.description(description))
    }
    def transformWithoutAnnotations[B](f: A => B, g: B => A)(implicit loc: SourceLocation): Schema[B] =
      Schema.Transform[A, B, SourceLocation](schema, a => Right(f(a)), b => Right(g(b)), Chunk.empty, loc)
  }

  object Validators {
    val nonEmptyStringValidator: Validator.Primitive[String] = Validator.minLength(1)
    val posIntValidator: Validator.Primitive[Int] = Validator.min(1)
    val nonNegIntValidator: Validator.Primitive[Int] = Validator.min(0)

    def nonEmptyCollectionValidator[A, C[_] <: Iterable[_]]: Validator[C[A]] = Validator.minSize(1)
  }

  implicit val nonEmptyStringSchema: Schema[NonEmptyString] =
    Schema[String]
      .validate(Validators.nonEmptyStringValidator)
      .transformWithoutAnnotations[NonEmptyString](NonEmptyString.unsafeFrom, _.value)

  implicit val posIntSchema: Schema[PosInt] =
    Schema[Int]
      .validate(Validators.posIntValidator)
      .transformWithoutAnnotations[PosInt](PosInt.unsafeFrom, _.value)

  implicit val nonNegIntSchema: Schema[NonNegInt] =
    Schema[Int]
      .validate(Validators.nonNegIntValidator)
      .transformWithoutAnnotations[NonNegInt](NonNegInt.unsafeFrom, _.value)

  implicit val instantTypeSchema: InstantType = InstantType(DateTimeFormatter.ISO_INSTANT)

  implicit def nonEmptyListSchema[A: Schema]: Schema[NonEmptyList[A]] =
    Schema[List[A]]
      .validate(Validators.nonEmptyCollectionValidator)
      .transformWithoutAnnotations(NonEmptyList.fromListUnsafe, _.toList)
}
