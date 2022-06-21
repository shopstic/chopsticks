package dev.chopsticks.openapi

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

  implicit val instantTypeSchema: InstantType = InstantType(DateTimeFormatter.ISO_INSTANT)

  implicit def nonEmptyListSchema[A: Schema]: Schema[OpenApiNonEmptyList[A]] =
    Schema[List[A]]
      .validate(Validators.nonEmptyCollectionValidator)
      .transformWithoutAnnotations(OpenApiNonEmptyList(_), _.value)
}
