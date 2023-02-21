package dev.chopsticks.schema

import eu.timepit.refined.types.numeric.{NonNegDouble, NonNegInt, PosDouble, PosInt}
import eu.timepit.refined.types.string.NonEmptyString
import squants.information.{DataRate, Information}
import sttp.tapir.Validator
import zio.Chunk
import zio.schema.Schema
import zio.schema.internal.SourceLocation

import scala.deriving.Mirror
import scala.util.{Failure, Success, Try}

trait Schemas:

  extension [A](schema: Schema[A])
    def named(name: String): Schema[A] =
      schema.annotate(SchemaAnnotation.EntityName(name))
    def validate(validator: Validator[A]): Schema[A] =
      schema.annotate(SchemaAnnotation.Validate[A](validator))
    def description(description: String): Schema[A] =
      schema.annotate(SchemaAnnotation.Description(description))
    def default(value: A, encodedValue: Option[Any] = None): Schema[A] =
      schema.annotate(SchemaAnnotation.Default(value, encodedValue))
    def discriminator(discriminator: SchemaDiscriminator[A]): Schema[A] =
      schema.annotate(SchemaAnnotation.SumTypeSerDeStrategy(SchemaSumTypeSerDeStrategy.Discriminator(discriminator)))
    inline def withDerivedDiscriminator(using m: Mirror.SumOf[A]): Schema[A] =
      schema.discriminator(SchemaDiscriminator.derive[A])
    //    inline def withDerivedDiscriminator(using m: Mirror.SumOf[A])(
    //      transformSubTypeNames: Tuple.Union[m.MirroredElemLabels] => String
    //    )(using Tuple.Union[m.MirroredElemLabels] <:< String): Schema[A] =
    //      schema.discriminator(
    //        SchemaDiscriminator
    //          .derive[A]
    //          .transformSubTypeNames((x: Tuple.Union[m.MirroredElemLabels]) =>
    //            transformSubTypeNames.asInstanceOf[Tuple.Union[m.MirroredElemLabels] => String](
    //              x.asInstanceOf[Tuple.Union[m.MirroredElemLabels]]
    //            )
    //          )
    //      )
    def mapBoth[B](f: A => B, g: B => A)(implicit loc: SourceLocation): Schema[B] =
      Schema.Transform[A, B, SourceLocation](schema, a => Right(f(a)), b => Right(g(b)), Chunk.empty, loc)
    def mapBothTry[B](f: A => Try[B], g: B => A)(implicit loc: SourceLocation): Schema[B] =
      Schema.Transform[A, B, SourceLocation](
        schema,
        a => {
          f(a) match
            case Failure(exception) => Left(exception.getMessage)
            case Success(value) => Right(value)
        },
        b => Right(g(b)),
        Chunk.empty,
        loc
      )
    def transformWithoutAnnotations[B](f: A => Either[String, B], g: B => A)(implicit
      loc: SourceLocation
    ): Schema[B] =
      Schema.Transform[A, B, SourceLocation](schema, a => f(a), b => Right(g(b)), Chunk.empty, loc)
    def withJsonFieldsCaseConverter(from: SchemaNamingConvention, to: SchemaNamingConvention): Schema[A] =
      schema.annotate(SchemaAnnotation.JsonCaseConverter(from, to))
    def withSnakeCaseJsonFields: Schema[A] =
      withJsonFieldsCaseConverter(SchemaNamingConvention.CamelCase, SchemaNamingConvention.SnakeCase)
    def withConfigFieldsCaseConverter(from: SchemaNamingConvention, to: SchemaNamingConvention): Schema[A] =
      schema.annotate(SchemaAnnotation.ConfigCaseConverter(from, to))
    def withKebabCaseConfigFields: Schema[A] =
      withConfigFieldsCaseConverter(SchemaNamingConvention.CamelCase, SchemaNamingConvention.SnakeCase)

// information
  implicit val informationSchema: Schema[Information] =
    summon[Schema[String]].mapBothTry(Information.apply, _.toString)
  implicit val dataRateConfigConvert: Schema[DataRate] =
    summon[Schema[String]].mapBothTry(DataRate.apply, _.toString)

  implicit val posIntSchema: Schema[PosInt] =
    summon[Schema[Int]].transformWithoutAnnotations[PosInt](PosInt.from, _.value)
  implicit val nonNegIntSchema: Schema[NonNegInt] =
    summon[Schema[Int]].transformWithoutAnnotations[NonNegInt](NonNegInt.from, _.value)
  implicit val nonEmptyStringSchema: Schema[NonEmptyString] =
    summon[Schema[String]].transformWithoutAnnotations[NonEmptyString](NonEmptyString.from, _.value)
  implicit val posDoubleSchema: Schema[PosDouble] =
    summon[Schema[Double]].transformWithoutAnnotations[PosDouble](PosDouble.from, _.value)
  implicit val nonNegDoubleSchema: Schema[NonNegDouble] =
    summon[Schema[Double]].transformWithoutAnnotations[NonNegDouble](NonNegDouble.from, _.value)

end Schemas

object Schemas extends Schemas
