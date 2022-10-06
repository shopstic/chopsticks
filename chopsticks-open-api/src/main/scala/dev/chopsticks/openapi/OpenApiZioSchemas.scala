package dev.chopsticks.openapi

import cats.data.NonEmptyList
import eu.timepit.refined.types.all.NonNegInt
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
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
      schema.annotate(OpenApiAnnotations.validate[A](validator))
    }
    def description(description: String): Schema[A] = {
      schema.annotate(OpenApiAnnotations.description(description))
    }
    def default(value: A, encodedValue: Option[Any] = None): Schema[A] = {
      schema.annotate(OpenApiAnnotations.default(value, encodedValue))
    }
    def discriminator(discriminator: OpenApiDiscriminator[A]): Schema[A] = {
      schema.annotate(OpenApiAnnotations.sumTypeSerDeStrategy(OpenApiSumTypeSerDeStrategy.Discriminator(discriminator)))
    }
    def mapBoth[B](f: A => B, g: B => A)(implicit loc: SourceLocation): Schema[B] =
      Schema.Transform[A, B, SourceLocation](schema, a => Right(f(a)), b => Right(g(b)), Chunk.empty, loc)
    def transformWithoutAnnotations[B](f: A => Either[String, B], g: B => A)(implicit loc: SourceLocation): Schema[B] =
      Schema.Transform[A, B, SourceLocation](schema, a => f(a), b => Right(g(b)), Chunk.empty, loc)
    def withJsonFieldsCaseConverter(from: OpenApiNamingConvention, to: OpenApiNamingConvention): Schema[A] =
      schema.annotate(OpenApiAnnotations.jsonCaseConverter(from, to))
    def withSnakeCaseJsonFields: Schema[A] =
      withJsonFieldsCaseConverter(OpenApiNamingConvention.OpenApiCamelCase, OpenApiNamingConvention.OpenApiSnakeCase)

  }

  object Validators {
    val nonEmptyStringValidator: Validator.Primitive[String] = Validator.minLength(1)
    val posIntValidator: Validator.Primitive[Int] = Validator.min(1)
    val nonNegIntValidator: Validator.Primitive[Int] = Validator.min(0)
    val posLongValidator: Validator.Primitive[Long] = Validator.min(1L)
    val nonNegLongValidator: Validator.Primitive[Long] = Validator.min(0L)

    def nonEmptyCollectionValidator[A, C[_] <: Iterable[_]]: Validator[C[A]] = Validator.minSize(1)
  }

  implicit val nonEmptyStringSchema: Schema[NonEmptyString] =
    Schema[String]
      .validate(Validators.nonEmptyStringValidator)
      .transformWithoutAnnotations[NonEmptyString](NonEmptyString.from, _.value)

  implicit val posIntSchema: Schema[PosInt] =
    Schema[Int]
      .validate(Validators.posIntValidator)
      .transformWithoutAnnotations[PosInt](PosInt.from, _.value)

  implicit val nonNegIntSchema: Schema[NonNegInt] =
    Schema[Int]
      .validate(Validators.nonNegIntValidator)
      .transformWithoutAnnotations[NonNegInt](NonNegInt.from, _.value)

  implicit val posLongSchema: Schema[PosLong] =
    Schema[Long]
      .validate(Validators.posLongValidator)
      .transformWithoutAnnotations[PosLong](PosLong.from, _.value)

  implicit val nonNegLongSchema: Schema[NonNegLong] =
    Schema[Long]
      .validate(Validators.nonNegLongValidator)
      .transformWithoutAnnotations[NonNegLong](NonNegLong.from, _.value)

  implicit val instantTypeSchema: InstantType = InstantType(DateTimeFormatter.ISO_INSTANT)

  implicit def nonEmptyListSchema[A: Schema]: Schema[NonEmptyList[A]] =
    Schema[List[A]]
      .validate(Validators.nonEmptyCollectionValidator)
      .transformWithoutAnnotations(
        xs => {
          NonEmptyList.fromList(xs) match {
            case Some(v) => Right(v)
            case None => Left("Provided array should have at least one element")
          }
        },
        _.toList
      )
}
