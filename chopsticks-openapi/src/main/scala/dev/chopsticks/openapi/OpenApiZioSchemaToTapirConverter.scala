package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiParsedAnnotations.extractAnnotations
import dev.chopsticks.openapi.common.OpenApiConverterUtils
import sttp.tapir.{FieldName, Schema => TapirSchema, SchemaType}
import sttp.tapir.Schema.SName
import sttp.tapir.SchemaType.{SDiscriminator, SOption, SRef, SchemaWithValue}
import zio.schema.{Schema => ZioSchema, StandardType, TypeId}
import zio.Chunk

import scala.collection.compat.immutable.ArraySeq

object OpenApiZioSchemaToTapirConverter {
  def convert[A](zioSchema: ZioSchema[A]): TapirSchema[A] = {
    new Converter(scala.collection.mutable.Map.empty).convert(zioSchema)
  }

  final case class CacheValue(schemaName: SName, description: Option[String], default: Option[(Any, Option[Any])])

  private class Converter(cache: scala.collection.mutable.Map[String, CacheValue]) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: OpenApiParsedAnnotations[A]
    )(convert: => TapirSchema[A]): TapirSchema[A] = {
      val entityName = OpenApiConverterUtils.getEntityName(Some(typeId), annotations)
      entityName match {
        case Some(name) =>
          cache.get(name) match {
            case Some(value) =>
              if (value.description != annotations.description) {
                throw new IllegalArgumentException(
                  s"There exists two schemas with the same name [$name], but different descriptions. " +
                    s"1st description: [${value.description.getOrElse("")}] " +
                    s"2nd description: [${annotations.description.getOrElse("")}]"
                )
              }
              if (value.default != annotations.default) {
                throw new IllegalArgumentException(
                  s"There exists two schemas with the same name [$name], but different default values. " +
                    s"1st default: [${value.default}] " +
                    s"2nd default: [${annotations.default}]"
                )
              }
              TapirSchema(schemaType = SRef(value.schemaName))
            case None =>
              val sname = schemaName(name)
              val _ = cache.addOne(name -> CacheValue(sname, annotations.description, annotations.default))
              val result = convert
              // to derive exactly the same schema as tapir does, uncomment the line below; however it's redundant in practice
//              val _ = cache.remove(name)
              result
          }
        case None =>
          convert
      }
    }

    def convert[A](zioSchema: ZioSchema[A]): TapirSchema[A] = {
      //scalafmt: { maxColumn = 400, optIn.configStyleArguments =false }
      zioSchema match {
        case ZioSchema.Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case ZioSchema.Sequence(schemaA, _, toChunk, annotations, _) =>
          addAnnotations(
            typeId = None,
            TapirSchema(SchemaType.SArray(convert(schemaA))(toChunk)),
            extractAnnotations(annotations)
          )

        case ZioSchema.Map(_, _, _) =>
          ???

        case ZioSchema.Set(schema, annotation) =>
          addAnnotations(
            typeId = None,
            TapirSchema(SchemaType.SArray(convert(schema))(_.asInstanceOf[Iterable[_]])),
            extractAnnotations(annotation)
          )

        case ZioSchema.Transform(schema, f, g, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseSchema = convert(schema)
            .map(x => f(x).fold(_ => None, Some(_))) { a =>
              g(a) match {
                case Left(error) => throw new RuntimeException(s"Couldn't convert schema: $error")
                case Right(value) => value
              }
            }
          addAnnotations(typeId = None, baseSchema, typedAnnotations, skipName = true)

        case ZioSchema.Tuple2(_, _, _) =>
          ???

        case ZioSchema.Optional(schema, annotations) =>
          addAnnotations[A](
            typeId = None,
            baseSchema = {
              val underlying = convert(schema)
              TapirSchema[A](
                schemaType = SOption(underlying)(x => x.asInstanceOf[Option[_]]),
                isOptional = true,
                format = underlying.format,
                deprecated = underlying.deprecated
              )
            },
            metadata = extractAnnotations(annotations)
          )

        case ZioSchema.Fail(_, _) =>
          ???

        case either @ ZioSchema.Either(_, _, _) =>
          convert(either.toEnum).as[A]

        case l @ ZioSchema.Lazy(_) =>
          convert(l.schema)

        case s: ZioSchema.Record[A] =>
          convertCaseClass[A](s.id, s.annotations, s.fields)

        case s: ZioSchema.Enum[A] =>
          convertEnum[A](s.id, s.annotations, s.cases: _*)

        case _ =>
          ???
      }
      //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
    }

    private def convertCaseClass[A](
      id: TypeId,
      annotations: Chunk[Any],
      fields: Chunk[ZioSchema.Field[A, _]]
    ): TapirSchema[A] = {
      val caseClassAnnotations = extractAnnotations[A](annotations)
      convertUsingCache(id, caseClassAnnotations) {
        val tapirFields: List[SchemaType.SProductField[A]] = fields.iterator
          .map { field =>
            val schema = addAnnotations(
              typeId = None,
              convert(field.schema).asInstanceOf[TapirSchema[Any]],
              extractAnnotations(field.annotations)
            )
            val transformedLabel = caseClassAnnotations.transformJsonLabel(field.name.toString)
            SchemaType.SProductField(
              _name = FieldName(transformedLabel, transformedLabel),
              _schema = schema,
              _get = (a: A) => Some(field.get(a))
            )
          }
          .toList
        addAnnotations(
          typeId = Some(id),
          TapirSchema(SchemaType.SProduct[A](tapirFields)),
          caseClassAnnotations
        )
      }
    }

    private def convertEnum[A](
      typeId: TypeId,
      annotations: Chunk[Any],
      cases: ZioSchema.Case[A, _]*
    ): TapirSchema[A] = {
      val enumAnnotations = extractAnnotations[A](annotations)
      val schemas = cases.iterator
        .map { c => convert(c.schema) }
        .to(ArraySeq)
      addAnnotations(
        typeId = Some(typeId),
        baseSchema = TapirSchema(
          SchemaType.SCoproduct[A](
            schemas.toList,
            discriminator = enumAnnotations.sumTypeSerDeStrategy.flatMap {
              case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
                Some(SDiscriminator(
                  FieldName(
                    name = discriminator.discriminatorFieldName,
                    encodedName = discriminator.discriminatorFieldName
                  ),
                  mapping = discriminator.mapping.view.mapValues(entityName => SRef(SName(entityName))).toMap
                ))
            }
          ) { a =>
            var i = 0
            var result = Option.empty[TapirSchema[_]]
            while (i < schemas.length && result.isEmpty) {
              val c = cases(i)
              result = c.deconstructOption(a) match {
                case Some(_) => Some(schemas(i))
                case None => result
              }
              i += 1
            }
            result.map(s => SchemaWithValue(s.asInstanceOf[TapirSchema[Any]], a.asInstanceOf[Any]))
          }
        ),
        enumAnnotations
      )
    }

    private def addAnnotations[A](
      typeId: Option[TypeId],
      baseSchema: TapirSchema[A],
      metadata: OpenApiParsedAnnotations[A],
      skipName: Boolean = false
    ): TapirSchema[A] = {
      var result = baseSchema
      val entityName = OpenApiConverterUtils.getEntityName(typeId, metadata)
      if (entityName.isDefined && !skipName) {
        result = baseSchema.copy(name = entityName.map(schemaName))
      }
      if (metadata.description.isDefined) {
        result = result.copy(description = metadata.description)
      }
      result = metadata.validator.fold(result) { validator =>
        result.copy(validator = validator)
      }
      result = metadata.default.fold(result) { case (default, encodedDefault) =>
        result.default(default, encodedDefault)
      }
      result
    }

    private def schemaName(entityName: String): SName = SName(entityName)

    private def primitiveConverter[A](standardType: StandardType[A], annotations: Chunk[Any]): TapirSchema[A] = {
      val schemaType: SchemaType[A] = standardType match {
        case StandardType.UnitType => SchemaType.SProduct(List.empty)
        case StandardType.StringType => SchemaType.SString()
        case StandardType.BoolType => SchemaType.SBoolean()
        case StandardType.ByteType => SchemaType.SInteger()
        case StandardType.ShortType => SchemaType.SInteger()
        case StandardType.IntType => SchemaType.SInteger()
        case StandardType.LongType => SchemaType.SNumber()
        case StandardType.FloatType => SchemaType.SNumber()
        case StandardType.DoubleType => SchemaType.SNumber()
        case StandardType.BinaryType => SchemaType.SBinary()
        case StandardType.CharType => SchemaType.SString()
        case StandardType.BigIntegerType => SchemaType.SNumber()
        case StandardType.BigDecimalType => SchemaType.SNumber()
        case StandardType.UUIDType => SchemaType.SString()
        case StandardType.DayOfWeekType => SchemaType.SInteger()
        case StandardType.DurationType => SchemaType.SString()
        case StandardType.InstantType => SchemaType.SDateTime()
        case StandardType.LocalDateType => SchemaType.SDate()
        case StandardType.LocalDateTimeType => SchemaType.SDateTime()
        case StandardType.LocalTimeType => SchemaType.SString()
        case StandardType.MonthType => SchemaType.SString()
        case StandardType.MonthDayType => SchemaType.SString()
        case StandardType.OffsetDateTimeType => SchemaType.SDateTime()
        case StandardType.OffsetTimeType => SchemaType.SString()
        case StandardType.PeriodType => SchemaType.SString()
        case StandardType.YearType => SchemaType.SString()
        case StandardType.YearMonthType => SchemaType.SString()
        case StandardType.ZonedDateTimeType => SchemaType.SDateTime()
        case StandardType.ZoneIdType => SchemaType.SString()
        case StandardType.ZoneOffsetType => SchemaType.SString()
        case StandardType.CurrencyType => SchemaType.SString()
      }
      addAnnotations(typeId = None, TapirSchema[A](schemaType = schemaType), extractAnnotations(annotations))
    }
  }
}
