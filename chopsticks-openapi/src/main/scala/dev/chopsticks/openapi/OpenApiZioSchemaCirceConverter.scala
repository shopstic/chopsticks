package dev.chopsticks.openapi

import cats.data.{NonEmptyList, Validated}
import dev.chopsticks.openapi.OpenApiParsedAnnotations.extractAnnotations
import dev.chopsticks.openapi.common.{ConverterCache, OpenApiConverterUtils}
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json, JsonObject}
import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe.Encoder.AsObject
import sttp.tapir.Validator
import zio.schema.{Schema => ZioSchema, StandardType}
import zio.Chunk

import java.math.BigInteger
import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
  Period,
  Year,
  YearMonth,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.UUID
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.language.existentials

object OpenApiZioSchemaCirceConverter {

  object Decoder {
    def convert[A](zioSchema: ZioSchema[A]): Decoder[A] = {
      new Converter().convert(zioSchema)
    }

    // allows non-empty objects and arrays to be decoded as Unit
    implicit final private val decodeUnit: Decoder[Unit] = new Decoder[Unit] {
      final def apply(c: HCursor): Result[Unit] = c.value.fold(
        jsonNull = Right(()),
        jsonBoolean = _ => Left(DecodingFailure("Unit", c.history)),
        jsonNumber = _ => Left(DecodingFailure("Unit", c.history)),
        jsonString = _ => Left(DecodingFailure("Unit", c.history)),
        jsonArray = _ => Right(()),
        jsonObject = _ => Right(())
      )
    }

    private def decodeCaseClass0[A](construct: () => A): Decoder[A] = {
      decodeUnit.map(_ => construct())
    }

    final private[openapi] class LazyDecoder[A] extends ConverterCache.Lazy[io.circe.Decoder[A]]
        with io.circe.Decoder[A] {
      override def apply(c: HCursor): Result[A] = get(c)
    }

    private class Converter(
      cache: ConverterCache[io.circe.Decoder] = new ConverterCache[io.circe.Decoder]()
    ) {
      private def convertUsingCache[A](schema: ZioSchema[A])(convert: => Decoder[A]): Decoder[A] = {
        cache.convertUsingCache(schema)(convert)(() => new LazyDecoder[A])
      }

      def convert[A](zioSchema: ZioSchema[A]): Decoder[A] = {
        //scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
        zioSchema match {
          case ZioSchema.Primitive(standardType, annotations) =>
            primitiveConverter(standardType, annotations)

          case ZioSchema.Sequence(schemaA, fromChunk, _, annotations, _) =>
            addAnnotations(
              OpenApiCirceCodecs.decodeChunk(convert(schemaA)).map(fromChunk),
              extractAnnotations(annotations)
            )

          case ZioSchema.Map(_, _, _) =>
            ???

          case ZioSchema.Set(schema, annotation) =>
            addAnnotations(
              io.circe.Decoder.decodeSet(convert(schema)),
              extractAnnotations(annotation)
            )

          case ZioSchema.Transform(schema, f, _, annotations, _) =>
            val typedAnnotations = extractAnnotations[A](annotations)
            val baseDecoder = convert(schema).emap(f)
            addAnnotations(baseDecoder, typedAnnotations)

          case ZioSchema.Tuple2(_, _, _) =>
            ???

          case ZioSchema.Optional(schema, annotations) =>
            addAnnotations[A](
              baseDecoder = io.circe.Decoder.decodeOption(convert(schema)).asInstanceOf[Decoder[A]],
              metadata = extractAnnotations(annotations)
            )

          case ZioSchema.Fail(_, _) =>
            ???

          case s @ ZioSchema.GenericRecord(_, _, _) =>
            genericRecordConverter(s)

          case either @ ZioSchema.Either(_, _, _) =>
            convert(either.toEnum).asInstanceOf[Decoder[A]]

          case l @ ZioSchema.Lazy(_) =>
            convert(l.schema)

          case ZioSchema.CaseClass0(_, construct, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(zioSchema) {
              val baseDecoder = decodeCaseClass0(construct)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass1(_, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field.schema), extractAnnotations(s.field.annotations))
              val baseDecoder = io.circe.Decoder.forProduct1(parsed.transformJsonLabel(s.field.name.toString))(s.defaultConstruct)(decoder1)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass2(_, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val baseDecoder = io.circe.Decoder.forProduct2(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString)
              )(s.construct)(decoder1, decoder2)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass3(_, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val baseDecoder = io.circe.Decoder.forProduct3(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass4(_, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val baseDecoder = io.circe.Decoder.forProduct4(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass5(_, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val baseDecoder = io.circe.Decoder.forProduct5(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass6(_, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val baseDecoder = io.circe.Decoder.forProduct6(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass7(_, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val baseDecoder = io.circe.Decoder.forProduct7(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val baseDecoder = io.circe.Decoder.forProduct8(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val baseDecoder = io.circe.Decoder.forProduct9(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val baseDecoder = io.circe.Decoder.forProduct10(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val baseDecoder = io.circe.Decoder.forProduct11(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val baseDecoder = io.circe.Decoder.forProduct12(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val baseDecoder = io.circe.Decoder.forProduct13(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val baseDecoder = io.circe.Decoder.forProduct14(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val baseDecoder = io.circe.Decoder.forProduct15(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val baseDecoder = io.circe.Decoder.forProduct16(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val baseDecoder = io.circe.Decoder.forProduct17(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val decoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val baseDecoder = io.circe.Decoder.forProduct18(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val decoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val decoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val baseDecoder = io.circe.Decoder.forProduct19(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val decoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val decoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val decoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val baseDecoder = io.circe.Decoder.forProduct20(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val decoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val decoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val decoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val decoder21 = addAnnotations(convert(s.field21.schema), extractAnnotations(s.field21.annotations))
              val baseDecoder = io.circe.Decoder.forProduct21(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString),
                parsed.transformJsonLabel(s.field21.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21)
              addAnnotations(baseDecoder, parsed)
            }

          case s @ ZioSchema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val decoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val decoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val decoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val decoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val decoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val decoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val decoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val decoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val decoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val decoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val decoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val decoder21 = addAnnotations(convert(s.field21.schema), extractAnnotations(s.field21.annotations))
              val decoder22 = addAnnotations(convert(s.field22.schema), extractAnnotations(s.field22.annotations))
              val baseDecoder = io.circe.Decoder.forProduct22(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString),
                parsed.transformJsonLabel(s.field21.name.toString),
                parsed.transformJsonLabel(s.field22.name.toString)
              )(s.construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21, decoder22)
              addAnnotations(baseDecoder, parsed)
            }

          case s: ZioSchema.Enum[A] =>
            convertEnum[A](s.annotations, s.cases: _*)

          case _ =>
            ???
        }
        //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
      }

      private def convertEnum[A](
        annotations: Chunk[Any],
        cases: ZioSchema.Case[A, _]*
      ): Decoder[A] = {
        val enumAnnotations = extractAnnotations[A](annotations)
        val decodersByName = cases.iterator
          .map { c =>
            val cAnn = extractAnnotations(c.annotations)
            val decoder = addAnnotations(
              convert(c.schema),
              extractAnnotations(c.annotations)
            ).asInstanceOf[io.circe.Decoder[Any]]
            val entityName = OpenApiConverterUtils.getCaseEntityName(c, cAnn).getOrElse(throw new RuntimeException(
              s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined or be a case class to derive an io.circe.Decoder. Received annotations: $cAnn"
            ))
            entityName -> decoder
          }
          .toMap
        val discriminator = enumAnnotations.sumTypeSerDeStrategy

        val decoder = discriminator
          .getOrElse(throw new RuntimeException(
            s"Discriminator must be defined to derive an io.circe.Decoder. Received annotations: $enumAnnotations"
          )) match {
          case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
            val diff = discriminator.mapping.values.toSet.diff(decodersByName.keySet)
            if (diff.nonEmpty) {
              throw new RuntimeException(
                s"Cannot derive io.circe.Decoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and decoders don't match. Diff=$diff."
              )
            }
            new io.circe.Decoder[A] {
              private val knownObjectTypes = discriminator.mapping.keys.toList.sorted.mkString(", ")
              override def apply(c: HCursor): Result[A] = {
                val discTypeCursor = c.downField(discriminator.discriminatorFieldName)
                discTypeCursor.as[String].flatMap { value =>
                  discriminator.mapping.get(value) match {
                    case None =>
                      Left(
                        DecodingFailure(
                          s"Unrecognized object type: ${value}. Valid object types are: $knownObjectTypes.",
                          discTypeCursor.history
                        )
                      )
                    case Some(objType) =>
                      decodersByName(objType).apply(c).asInstanceOf[Result[A]]
                  }
                }
              }
            }
        }
        addAnnotations(decoder, enumAnnotations)
      }

      private def genericRecordConverter(
        schema: ZioSchema.GenericRecord
      ): Decoder[ListMap[String, _]] = {
        val annotations = schema.annotations
        val fieldSet = schema.fieldSet
        val parsed = extractAnnotations[ListMap[String, _]](annotations)
        convertUsingCache(schema) {
          val fieldDecoders = fieldSet.toChunk.iterator
            .map { field =>
              val fieldDecoder = addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
              parsed.transformJsonLabel(field.name.toString) -> (field.name.toString, fieldDecoder)
            }
            .toMap
          val baseDecoder = new Decoder[ListMap[String, _]] {
            override def apply(c: HCursor): Result[ListMap[String, _]] = {
              decodeWithCursor(c, accumulate = false) match {
                case Left(errors) => Left(errors.head)
                case Right(value) => Right(value)
              }
            }

            override def decodeAccumulating(c: HCursor): AccumulatingResult[ListMap[String, _]] = {
              decodeWithCursor(c, accumulate = true) match {
                case Left(errors) => Validated.Invalid(NonEmptyList.fromListUnsafe(errors))
                case Right(value) => Validated.Valid(value)
              }
            }

            private def decodeWithCursor(
              c: HCursor,
              accumulate: Boolean
            ): Either[List[DecodingFailure], ListMap[String, _]] = {
              val iter = fieldDecoders.iterator
              val errors = ListBuffer.empty[DecodingFailure]
              val builder = ListMap.newBuilder[String, Any]
              while (iter.hasNext && (errors.isEmpty || accumulate)) {
                val (mappedKey, (key, decoder)) = iter.next()
                if (accumulate) {
                  val result: AccumulatingResult[Any] = decoder.tryDecodeAccumulating(c.downField(mappedKey))
                  result match {
                    case Validated.Invalid(failures) => val _ = errors.addAll(failures.iterator)
                    case Validated.Valid(value) => val _ = builder.addOne(key -> value)
                  }
                }
                else {
                  val result: Result[Any] = decoder.tryDecode(c.downField(mappedKey))
                  result match {
                    case Left(failure) => val _ = errors.addOne(failure)
                    case Right(value) => val _ = builder.addOne(key -> value)
                  }
                }
              }
              if (errors.isEmpty) Right(builder.result())
              else Left(errors.toList)
            }
          }
          addAnnotations(baseDecoder, parsed)
        }
      }

      private def primitiveConverter[A](standardType: StandardType[A], annotations: Chunk[Any]): Decoder[A] = {
        val baseDecoder = standardType match {
          case StandardType.UnitType => decodeUnit
          case StandardType.StringType => io.circe.Decoder[String]
          case StandardType.BoolType => io.circe.Decoder[Boolean]
          case StandardType.ByteType => io.circe.Decoder[Byte]
          case StandardType.ShortType => io.circe.Decoder[Short]
          case StandardType.IntType => io.circe.Decoder[Int]
          case StandardType.LongType => io.circe.Decoder[Long]
          case StandardType.FloatType => io.circe.Decoder[Float]
          case StandardType.DoubleType => io.circe.Decoder[Double]
          case StandardType.BinaryType => ???
          case StandardType.CurrencyType => ???
          case StandardType.CharType => io.circe.Decoder[String]
          case StandardType.BigIntegerType => io.circe.Decoder[BigInteger]
          case StandardType.BigDecimalType => io.circe.Decoder[java.math.BigDecimal]
          case StandardType.UUIDType => io.circe.Decoder[UUID]
          case StandardType.DayOfWeekType => io.circe.Decoder[Int] // todo add validation
          case StandardType.DurationType => io.circe.Decoder[String]
          case StandardType.InstantType => io.circe.Decoder[Instant]
          case StandardType.LocalDateType => io.circe.Decoder[LocalDate]
          case StandardType.LocalDateTimeType => io.circe.Decoder[LocalDateTime]
          case StandardType.LocalTimeType => io.circe.Decoder[LocalTime]
          case StandardType.MonthType => io.circe.Decoder[String] // todo add validation
          case StandardType.MonthDayType => io.circe.Decoder[String] // todo add validation
          case StandardType.OffsetDateTimeType => io.circe.Decoder[OffsetDateTime]
          case StandardType.OffsetTimeType => io.circe.Decoder[OffsetTime]
          case StandardType.PeriodType => io.circe.Decoder[Period]
          case StandardType.YearType => io.circe.Decoder[Year]
          case StandardType.YearMonthType => io.circe.Decoder[YearMonth]
          case StandardType.ZonedDateTimeType => io.circe.Decoder[ZonedDateTime]
          case StandardType.ZoneIdType => io.circe.Decoder[ZoneId]
          case StandardType.ZoneOffsetType => io.circe.Decoder[ZoneOffset]
        }
        addAnnotations(baseDecoder.asInstanceOf[Decoder[A]], extractAnnotations(annotations))
      }

      private def addAnnotations[A](
        baseDecoder: Decoder[A],
        metadata: OpenApiParsedAnnotations[A]
      ): Decoder[A] = {
        var decoder = baseDecoder
        decoder = metadata.default.fold(decoder) { case (default, _) =>
          io.circe.Decoder.decodeOption[A](decoder).map(maybeValue => maybeValue.getOrElse(default))
        }
        decoder = metadata.validator.fold(decoder) { validator: Validator[A] =>
          decoder.ensure { a =>
            validator(a).map(OpenApiValidation.errorMessage)
          }
        }
        metadata.jsonDecoder.getOrElse(decoder)
      }
    }
  }

  object Encoder {
    def convert[A](zioSchema: ZioSchema[A]): Encoder[A] = {
      new Converter().convert(zioSchema)
    }

    private val emptyJson = Json.obj()
    private val _caseClass0Encoder = new Encoder[Any] {
      override def apply(a: Any): Json = emptyJson
    }

    def caseClass0Encoder[A] = _caseClass0Encoder.asInstanceOf[Encoder[A]]

    final private[openapi] class LazyEncoder[A] extends ConverterCache.Lazy[io.circe.Encoder[A]]
        with io.circe.Encoder[A] {
      override def apply(a: A): Json = get(a)
    }

    private class Converter(cache: ConverterCache[io.circe.Encoder] = new ConverterCache[io.circe.Encoder]()) {

      private def convertUsingCache[A](schema: ZioSchema[A])(convert: => io.circe.Encoder[A]): io.circe.Encoder[A] = {
        cache.convertUsingCache(schema)(convert)(() => new LazyEncoder[A])
      }

      def convert[A](zioSchema: ZioSchema[A]): io.circe.Encoder[A] = {
        //scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
        zioSchema match {
          case ZioSchema.Primitive(standardType, annotations) =>
            primitiveConverter(standardType, annotations)

          case ZioSchema.Sequence(schemaA, _, toChunk, annotations, _) =>
            addAnnotations(
              OpenApiCirceCodecs.encodeChunk(convert(schemaA)).contramap(toChunk),
              extractAnnotations(annotations)
            )

          case ZioSchema.Map(_, _, _) =>
            ???

          case ZioSchema.Set(schema, annotation) =>
            addAnnotations(
              io.circe.Encoder.encodeSet(convert(schema)).asInstanceOf[Encoder[A]],
              extractAnnotations(annotation)
            )

          case ZioSchema.Transform(schema, _, g, annotations, _) =>
            val typedAnnotations = extractAnnotations[A](annotations)
            val baseSchema = convert(schema)
              .contramap[A] { x =>
                g(x) match {
                  case Right(v) => v
                  case Left(error) => throw new RuntimeException(s"Couldn't transform schema: $error")
                }
              }
            addAnnotations(baseSchema, typedAnnotations)

          case ZioSchema.Tuple2(_, _, _) =>
            ???

          case ZioSchema.Optional(schema, annotations) =>
            addAnnotations[A](
              baseEncoder = io.circe.Encoder.encodeOption(convert(schema)).asInstanceOf[Encoder[A]],
              metadata = extractAnnotations(annotations)
            )

          case ZioSchema.Fail(_, _) =>
            ???

          case ZioSchema.GenericRecord(_, fieldSet, annotations) =>
            val recordAnnotations: OpenApiParsedAnnotations[A] = extractAnnotations[A](annotations)
            convertUsingCache(zioSchema) {
              val fieldEncoders = fieldSet.toChunk
                .map { field =>
                  addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
                }
              val baseEncoder = new AsObject[ListMap[String, _]] {
                override def encodeObject(a: ListMap[String, _]): JsonObject = {
                  val record = a.iterator.zip(fieldEncoders.iterator)
                    .map { case ((k, v), encoder) =>
                      (recordAnnotations.transformJsonLabel(k), encoder.asInstanceOf[Encoder[Any]](v.asInstanceOf[Any]))
                    }
                    .toVector
                  JsonObject.fromIterable(record)
                }
              }
              addAnnotations(baseEncoder, recordAnnotations)
            }

          case either @ ZioSchema.Either(_, _, _) =>
            convert(either.toEnum).asInstanceOf[Encoder[A]]

          case l @ ZioSchema.Lazy(_) =>
            convert(l.schema)

          case ZioSchema.CaseClass0(_, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(zioSchema) {
              val baseEncoder = caseClass0Encoder[A]
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass1(_, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field.schema), extractAnnotations(s.field.annotations))
              val baseEncoder = io.circe.Encoder.forProduct1(
                parsed.transformJsonLabel(s.field.name.toString)
              )((a: A) => (s.field.get(a)))(encoder1)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass2(_, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val baseEncoder = io.circe.Encoder.forProduct2(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a)))(encoder1, encoder2)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass3(_, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val baseEncoder = io.circe.Encoder.forProduct3(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a)))(encoder1, encoder2, encoder3)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass4(_, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val baseEncoder = io.circe.Encoder.forProduct4(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a)))(encoder1, encoder2, encoder3, encoder4)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass5(_, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val baseEncoder = io.circe.Encoder.forProduct5(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass6(_, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val baseEncoder = io.circe.Encoder.forProduct6(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass7(_, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val baseEncoder = io.circe.Encoder.forProduct7(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val baseEncoder = io.circe.Encoder.forProduct8(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val baseEncoder = io.circe.Encoder.forProduct9(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val baseEncoder = io.circe.Encoder.forProduct10(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val baseEncoder = io.circe.Encoder.forProduct11(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val baseEncoder = io.circe.Encoder.forProduct12(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val baseEncoder = io.circe.Encoder.forProduct13(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val baseEncoder = io.circe.Encoder.forProduct14(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val baseEncoder = io.circe.Encoder.forProduct15(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val baseEncoder = io.circe.Encoder.forProduct16(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val baseEncoder = io.circe.Encoder.forProduct17(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val encoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val baseEncoder = io.circe.Encoder.forProduct18(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a), s.field18.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val encoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val encoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val baseEncoder = io.circe.Encoder.forProduct19(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a), s.field18.get(a), s.field19.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val encoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val encoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val encoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val baseEncoder = io.circe.Encoder.forProduct20(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a), s.field18.get(a), s.field19.get(a), s.field20.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val encoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val encoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val encoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val encoder21 = addAnnotations(convert(s.field21.schema), extractAnnotations(s.field21.annotations))
              val baseEncoder = io.circe.Encoder.forProduct21(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString),
                parsed.transformJsonLabel(s.field21.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a), s.field18.get(a), s.field19.get(a), s.field20.get(a), s.field21.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20, encoder21)
              addAnnotations(baseEncoder, parsed)
            }

          case s @ ZioSchema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            val parsed = extractAnnotations[A](s.annotations)
            convertUsingCache(zioSchema) {
              val encoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
              val encoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
              val encoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
              val encoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
              val encoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
              val encoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
              val encoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
              val encoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
              val encoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
              val encoder10 = addAnnotations(convert(s.field10.schema), extractAnnotations(s.field10.annotations))
              val encoder11 = addAnnotations(convert(s.field11.schema), extractAnnotations(s.field11.annotations))
              val encoder12 = addAnnotations(convert(s.field12.schema), extractAnnotations(s.field12.annotations))
              val encoder13 = addAnnotations(convert(s.field13.schema), extractAnnotations(s.field13.annotations))
              val encoder14 = addAnnotations(convert(s.field14.schema), extractAnnotations(s.field14.annotations))
              val encoder15 = addAnnotations(convert(s.field15.schema), extractAnnotations(s.field15.annotations))
              val encoder16 = addAnnotations(convert(s.field16.schema), extractAnnotations(s.field16.annotations))
              val encoder17 = addAnnotations(convert(s.field17.schema), extractAnnotations(s.field17.annotations))
              val encoder18 = addAnnotations(convert(s.field18.schema), extractAnnotations(s.field18.annotations))
              val encoder19 = addAnnotations(convert(s.field19.schema), extractAnnotations(s.field19.annotations))
              val encoder20 = addAnnotations(convert(s.field20.schema), extractAnnotations(s.field20.annotations))
              val encoder21 = addAnnotations(convert(s.field21.schema), extractAnnotations(s.field21.annotations))
              val encoder22 = addAnnotations(convert(s.field22.schema), extractAnnotations(s.field22.annotations))
              val baseEncoder = io.circe.Encoder.forProduct22(
                parsed.transformJsonLabel(s.field1.name.toString),
                parsed.transformJsonLabel(s.field2.name.toString),
                parsed.transformJsonLabel(s.field3.name.toString),
                parsed.transformJsonLabel(s.field4.name.toString),
                parsed.transformJsonLabel(s.field5.name.toString),
                parsed.transformJsonLabel(s.field6.name.toString),
                parsed.transformJsonLabel(s.field7.name.toString),
                parsed.transformJsonLabel(s.field8.name.toString),
                parsed.transformJsonLabel(s.field9.name.toString),
                parsed.transformJsonLabel(s.field10.name.toString),
                parsed.transformJsonLabel(s.field11.name.toString),
                parsed.transformJsonLabel(s.field12.name.toString),
                parsed.transformJsonLabel(s.field13.name.toString),
                parsed.transformJsonLabel(s.field14.name.toString),
                parsed.transformJsonLabel(s.field15.name.toString),
                parsed.transformJsonLabel(s.field16.name.toString),
                parsed.transformJsonLabel(s.field17.name.toString),
                parsed.transformJsonLabel(s.field18.name.toString),
                parsed.transformJsonLabel(s.field19.name.toString),
                parsed.transformJsonLabel(s.field20.name.toString),
                parsed.transformJsonLabel(s.field21.name.toString),
                parsed.transformJsonLabel(s.field22.name.toString)
              )((a: A) => (s.field1.get(a), s.field2.get(a), s.field3.get(a), s.field4.get(a), s.field5.get(a), s.field6.get(a), s.field7.get(a), s.field8.get(a), s.field9.get(a), s.field10.get(a), s.field11.get(a), s.field12.get(a), s.field13.get(a), s.field14.get(a), s.field15.get(a), s.field16.get(a), s.field17.get(a), s.field18.get(a), s.field19.get(a), s.field20.get(a), s.field21.get(a), s.field22.get(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20, encoder21, encoder22)
              addAnnotations(baseEncoder, parsed)
            }

          case s: ZioSchema.Enum[A] =>
            convertEnum[A](s.annotations, s.cases: _*)

          case _ =>
            ???
        }
        //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
      }

      private def convertEnum[A](
        annotations: Chunk[Any],
        cases: ZioSchema.Case[A, _]*
      ): Encoder[A] = {
        val enumAnnotations = extractAnnotations[A](annotations)
        val encodersByName = cases.iterator
          .map { c =>
            val cAnn = extractAnnotations(c.annotations)
            val encoder = addAnnotations(
              convert(c.schema),
              extractAnnotations(c.annotations)
            ).asInstanceOf[io.circe.Encoder[Any]]
            val entityName = OpenApiConverterUtils.getCaseEntityName(c, cAnn).getOrElse(
              throw new RuntimeException(
                s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined or be a case class to derive an io.circe.Encoder. Received annotations: $cAnn"
              )
            )
            entityName -> (encoder, c)
          }
          .toMap
        val discriminator = enumAnnotations.sumTypeSerDeStrategy

        val encoder = discriminator
          .getOrElse(throw new RuntimeException(
            s"Discriminator must be defined to derive an io.circe.Encoder. Received annotations: $enumAnnotations"
          )) match {
          case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
            val diff = discriminator.mapping.values.toSet.diff(encodersByName.keySet)
            if (diff.nonEmpty) {
              throw new RuntimeException(
                s"Cannot derive io.circe.Encoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and encoders don't match. Diff=$diff."
              )
            }
            new io.circe.Encoder[A] {
              override def apply(a: A): Json = {
                val discValue = discriminator.discriminatorValue(a)
                val (enc, c) = encodersByName(discriminator.mapping(discValue))
                val json = enc(c.deconstruct(a).asInstanceOf[Any])
                json.mapObject { o =>
                  o.add(discriminator.discriminatorFieldName, Json.fromString(discValue))
                }
              }
            }
        }
        addAnnotations(encoder, enumAnnotations)
      }

      private def primitiveConverter[A](standardType: StandardType[A], annotations: Chunk[Any]): Encoder[A] = {
        val baseEncoder = standardType match {
          case StandardType.UnitType => io.circe.Encoder[Unit]
          case StandardType.StringType => io.circe.Encoder[String]
          case StandardType.BoolType => io.circe.Encoder[Boolean]
          case StandardType.ByteType => io.circe.Encoder[Byte]
          case StandardType.ShortType => io.circe.Encoder[Short]
          case StandardType.IntType => io.circe.Encoder[Int]
          case StandardType.LongType => io.circe.Encoder[Long]
          case StandardType.FloatType => io.circe.Encoder[Float]
          case StandardType.DoubleType => io.circe.Encoder[Double]
          case StandardType.BinaryType => ???
          case StandardType.CurrencyType => ???
          case StandardType.CharType => io.circe.Encoder[String]
          case StandardType.BigIntegerType => io.circe.Encoder[BigInteger]
          case StandardType.BigDecimalType => io.circe.Encoder[java.math.BigDecimal]
          case StandardType.UUIDType => io.circe.Encoder[UUID]
          case StandardType.DayOfWeekType => io.circe.Encoder[Int] // todo add validation
          case StandardType.DurationType => io.circe.Encoder[String]
          case StandardType.InstantType => io.circe.Encoder[Instant]
          case StandardType.LocalDateType => io.circe.Encoder[LocalDate]
          case StandardType.LocalDateTimeType => io.circe.Encoder[LocalDateTime]
          case StandardType.LocalTimeType => io.circe.Encoder[LocalTime]
          case StandardType.MonthType => io.circe.Encoder[String] // todo add validation
          case StandardType.MonthDayType => io.circe.Encoder[String] // todo add validation
          case StandardType.OffsetDateTimeType => io.circe.Encoder[OffsetDateTime]
          case StandardType.OffsetTimeType => io.circe.Encoder[OffsetTime]
          case StandardType.PeriodType => io.circe.Encoder[Period]
          case StandardType.YearType => io.circe.Encoder[Year]
          case StandardType.YearMonthType => io.circe.Encoder[YearMonth]
          case StandardType.ZonedDateTimeType => io.circe.Encoder[ZonedDateTime]
          case StandardType.ZoneIdType => io.circe.Encoder[ZoneId]
          case StandardType.ZoneOffsetType => io.circe.Encoder[ZoneOffset]
        }
        addAnnotations(baseEncoder.asInstanceOf[Encoder[A]], extractAnnotations(annotations))
      }

      private def addAnnotations[A](
        baseEncoder: Encoder[A],
        metadata: OpenApiParsedAnnotations[A]
      ): Encoder[A] = {
        metadata.jsonEncoder.getOrElse(baseEncoder)
      }
    }
  }
}
