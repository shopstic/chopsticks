package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiParsedAnnotations.extractAnnotations
import sttp.tapir.{FieldName, Schema => TapirSchema, SchemaType}
import sttp.tapir.Schema.SName
import sttp.tapir.SchemaType.{SOption, SRef}
import zio.schema.{FieldSet, Schema => ZioSchema, StandardType}
import zio.Chunk

import scala.collection.immutable.ListMap

object OpenApiZioSchemaToTapirConverter {
  def convert[A](zioSchema: ZioSchema[A]): TapirSchema[A] = {
    new Converter(scala.collection.mutable.Map.empty).convert(zioSchema)
  }

  private class Converter(cache: scala.collection.mutable.Map[String, SName]) {
    private def convertUsingCache[A](annotations: OpenApiParsedAnnotations[A])(convert: => TapirSchema[A])
      : TapirSchema[A] = {
      annotations.entityName match {
        case Some(name) =>
          cache.get(name) match {
            case Some(sname) => TapirSchema(SRef(sname))
            case None =>
              val sname = schemaName(name)
              val _ = cache.addOne(name -> sname)
              val result = convert
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
            TapirSchema(SchemaType.SArray(convert(schemaA))(toChunk)),
            extractAnnotations(annotations)
          )

        case ZioSchema.MapSchema(_, _, _) =>
          ???

        case ZioSchema.SetSchema(schema, annotation) =>
          addAnnotations(
            TapirSchema(SchemaType.SArray(convert(schema))(_.asInstanceOf[Iterable[_]])), // todo check it
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
          addAnnotations(baseSchema, typedAnnotations)

        case ZioSchema.Tuple(_, _, _) =>
          ???

        case ZioSchema.Optional(schema, annotations) =>
          addAnnotations[A](
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

        case ZioSchema.GenericRecord(fieldSet, annotations) =>
          convertGenericRecord(fieldSet, annotations)

        case either @ ZioSchema.EitherSchema(_, _, _) =>
          convert(either.toEnum).as[A]

        case l @ ZioSchema.Lazy(_) =>
          convert(l.schema)

        case ZioSchema.Meta(_, _) =>
          ???

        case ZioSchema.CaseClass1(f1, _, ext1, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1)

        case ZioSchema.CaseClass2(f1, f2, _, ext1, ext2, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2)

        case ZioSchema.CaseClass3(f1, f2, f3, _, ext1, ext2, ext3, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3)

        case ZioSchema.CaseClass4(f1, f2, f3, f4, _, ext1, ext2, ext3, ext4, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4)

        case ZioSchema.CaseClass5(f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5)

        case ZioSchema.CaseClass6(f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6)

        case ZioSchema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7)

        case ZioSchema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8)

        case ZioSchema.CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9)

        case ZioSchema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10)

        case ZioSchema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11)

        case ZioSchema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12)

        case ZioSchema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13)

        case ZioSchema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14)

        case ZioSchema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15)

        case ZioSchema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16)

        case ZioSchema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17)

        case ZioSchema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18)

        case ZioSchema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19)

        case ZioSchema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20)

        case ZioSchema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21)

        case ZioSchema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22, annotations) =>
          convertCaseClass[A](annotations, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21, f22 -> ext22)

        // e.g. enums are not yet supported
        case _ =>
          ???
      }
      //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
    }

    private def convertGenericRecord(
      fieldSet: FieldSet,
      annotations: Chunk[Any]
    ): TapirSchema[ListMap[String, _]] = {
      val recordAnnotations = extractAnnotations[ListMap[String, _]](annotations)
      convertUsingCache(recordAnnotations) {
        val tapirFields = fieldSet.toChunk.iterator
          .map { field =>
            val schema = addAnnotations(
              convert(field.schema).asInstanceOf[TapirSchema[Any]],
              extractAnnotations(field.annotations)
            )
            SchemaType.SProductField(
              _name = FieldName(field.label, field.label),
              _schema = schema,
              _get = (a: ListMap[String, _]) => Some(a(field.label))
            )
          }
          .toList
        addAnnotations(
          TapirSchema(SchemaType.SProduct[ListMap[String, _]](tapirFields)),
          recordAnnotations
        )
      }
    }

    private def convertCaseClass[A](
      annotations: Chunk[Any],
      fields: (ZioSchema.Field[_], A => Any)*
    ): TapirSchema[A] = {
      val caseClassAnnotations = extractAnnotations[A](annotations)
      convertUsingCache(caseClassAnnotations) {
        val tapirFields: List[SchemaType.SProductField[A]] = fields.iterator
          .map { case (field, getField) =>
            val schema = addAnnotations(
              convert(field.schema).asInstanceOf[TapirSchema[Any]],
              extractAnnotations(field.annotations)
            )
            SchemaType.SProductField(
              _name = FieldName(field.label, field.label),
              _schema = schema,
              _get = (a: A) => Some(getField(a))
            )
          }
          .toList
        addAnnotations(
          TapirSchema(SchemaType.SProduct[A](tapirFields)),
          caseClassAnnotations
        )
      }
    }

    private def addAnnotations[A](
      baseSchema: TapirSchema[A],
      metadata: OpenApiParsedAnnotations[A]
    ): TapirSchema[A] = {
      var result = baseSchema
      if (metadata.entityName.isDefined) {
        result = baseSchema.copy(name = metadata.entityName.map(schemaName))
      }
      if (metadata.description.isDefined) {
        result = baseSchema.copy(description = metadata.description)
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
        case StandardType.UnitType => SchemaType.SString()
        case StandardType.StringType => SchemaType.SString()
        case StandardType.BoolType => SchemaType.SBoolean()
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
        case StandardType.InstantType(_) => SchemaType.SDateTime()
        case StandardType.LocalDateType(_) => SchemaType.SDate()
        case StandardType.LocalDateTimeType(_) => SchemaType.SDateTime()
        case StandardType.LocalTimeType(_) => SchemaType.SString()
        case StandardType.MonthType => SchemaType.SString()
        case StandardType.MonthDayType => SchemaType.SString()
        case StandardType.OffsetDateTimeType(_) => SchemaType.SDateTime()
        case StandardType.OffsetTimeType(_) => SchemaType.SString()
        case StandardType.PeriodType => SchemaType.SString()
        case StandardType.YearType => SchemaType.SString()
        case StandardType.YearMonthType => SchemaType.SString()
        case StandardType.ZonedDateTimeType(_) => SchemaType.SString()
        case StandardType.ZoneIdType => SchemaType.SString()
        case StandardType.ZoneOffsetType => SchemaType.SString()
      }
      addAnnotations(TapirSchema[A](schemaType = schemaType), extractAnnotations(annotations))
    }
  }
}
