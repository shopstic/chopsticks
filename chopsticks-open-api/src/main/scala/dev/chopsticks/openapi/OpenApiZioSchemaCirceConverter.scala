package dev.chopsticks.openapi

import cats.data.{NonEmptyList, Validated}
import dev.chopsticks.openapi.OpenApiParsedAnnotations.extractAnnotations
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json, JsonObject}
import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe.Encoder.AsObject
import sttp.tapir.{ValidationError, Validator}
import zio.schema.{FieldSet, Schema => ZioSchema, StandardType}
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
import scala.annotation.nowarn
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.language.existentials

object OpenApiZioSchemaCirceConverter {
  final private case class CacheKey(entityName: String, annotationsHash: Int)

  object Decoder {
    def convert[A](zioSchema: ZioSchema[A]): Decoder[A] = {
      new Converter(scala.collection.mutable.Map.empty).convert(zioSchema)
    }

    final private[openapi] class LazyDecoder[A]() extends io.circe.Decoder[A] {
      private var _decoder: io.circe.Decoder[A] = _
      private[Decoder] def set(encoder: io.circe.Decoder[A]): Unit =
        this._decoder = encoder
      private def get: io.circe.Decoder[A] =
        if (_decoder == null) throw new RuntimeException("LazyDecoder has not yet been initialized")
        else _decoder
      override def apply(c: HCursor): Result[A] = get(c)
    }

    private class Converter(cache: scala.collection.mutable.Map[CacheKey, LazyDecoder[_]]) {

      private def convertUsingCache[A](annotations: OpenApiParsedAnnotations[A])(convert: => Decoder[A]): Decoder[A] = {
        annotations.entityName match {
          case Some(name) =>
            val cacheKey = CacheKey(name, annotations.hashCode())
            cache.get(cacheKey) match {
              case Some(value) => value.asInstanceOf[io.circe.Decoder[A]]
              case None =>
                val lazyDec = new LazyDecoder[A]()
                val _ = cache.addOne(cacheKey -> lazyDec)
                val result = convert
                lazyDec.set(result)
                result
            }
          case None =>
            convert
        }
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

          case ZioSchema.MapSchema(_, _, _) =>
            ???

          case ZioSchema.SetSchema(schema, annotation) =>
            addAnnotations(
              io.circe.Decoder.decodeSet(convert(schema)),
              extractAnnotations(annotation)
            )

          case ZioSchema.Transform(schema, f, _, annotations, _) =>
            val typedAnnotations = extractAnnotations[A](annotations)
            val baseDecoder = convert(schema).emap(f)
            addAnnotations(baseDecoder, typedAnnotations)

          case ZioSchema.Tuple(_, _, _) =>
            ???

          case ZioSchema.Optional(schema, annotations) =>
            addAnnotations[A](
              baseDecoder = io.circe.Decoder.decodeOption(convert(schema)).asInstanceOf[Decoder[A]],
              metadata = extractAnnotations(annotations)
            )

          case ZioSchema.Fail(_, _) =>
            ???

          case ZioSchema.GenericRecord(fieldSet, annotations) =>
            genericRecordConverter(fieldSet, annotations)

          case either @ ZioSchema.EitherSchema(_, _, _) =>
            convert(either.toEnum).asInstanceOf[Decoder[A]]

          case l @ ZioSchema.Lazy(_) =>
            convert(l.schema)

          case ZioSchema.Meta(_, _) =>
            ???

          case ZioSchema.CaseClass1(f1, construct, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val baseDecoder = io.circe.Decoder.forProduct1(parsed.transformJsonLabel(f1.label))(construct)(decoder1)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass2(f1, f2, construct, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val baseDecoder = io.circe.Decoder.forProduct2(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label)
              )(construct)(decoder1, decoder2)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass3(f1, f2, f3, construct, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val baseDecoder = io.circe.Decoder.forProduct3(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label)
              )(construct)(decoder1, decoder2, decoder3)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass4(f1, f2, f3, f4, construct, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val baseDecoder = io.circe.Decoder.forProduct4(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass5(f1, f2, f3, f4, f5, construct, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val baseDecoder = io.circe.Decoder.forProduct5(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass6(f1, f2, f3, f4, f5, f6, construct, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val baseDecoder = io.circe.Decoder.forProduct6(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, construct, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val baseDecoder = io.circe.Decoder.forProduct7(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, construct, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val baseDecoder = io.circe.Decoder.forProduct8(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, construct, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val baseDecoder = io.circe.Decoder.forProduct9(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, construct, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val baseDecoder = io.circe.Decoder.forProduct10(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, construct, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val baseDecoder = io.circe.Decoder.forProduct11(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, construct, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val baseDecoder = io.circe.Decoder.forProduct12(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val baseDecoder = io.circe.Decoder.forProduct13(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val baseDecoder = io.circe.Decoder.forProduct14(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val baseDecoder = io.circe.Decoder.forProduct15(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val baseDecoder = io.circe.Decoder.forProduct16(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val baseDecoder = io.circe.Decoder.forProduct17(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val decoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val baseDecoder = io.circe.Decoder.forProduct18(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val decoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val decoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val baseDecoder = io.circe.Decoder.forProduct19(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val decoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val decoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val decoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val baseDecoder = io.circe.Decoder.forProduct20(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val decoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val decoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val decoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val decoder21 = addAnnotations(convert(f21.schema), extractAnnotations(f21.annotations))
              val baseDecoder = io.circe.Decoder.forProduct21(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label),
                parsed.transformJsonLabel(f21.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val decoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val decoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val decoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val decoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val decoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val decoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val decoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val decoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val decoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val decoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val decoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val decoder21 = addAnnotations(convert(f21.schema), extractAnnotations(f21.annotations))
              val decoder22 = addAnnotations(convert(f22.schema), extractAnnotations(f22.annotations))
              val baseDecoder = io.circe.Decoder.forProduct22(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label),
                parsed.transformJsonLabel(f21.label),
                parsed.transformJsonLabel(f22.label)
              )(construct)(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21, decoder22)
              addAnnotations(baseDecoder, parsed)
            }

          case ZioSchema.Enum1(c1, annotations) =>
            convertEnum[A](annotations, c1)

          case ZioSchema.Enum2(c1, c2, annotations) =>
            convertEnum[A](annotations, c1, c2)

          case ZioSchema.Enum3(c1, c2, c3, annotations) =>
            convertEnum[A](annotations, c1, c2, c3)

          case ZioSchema.Enum4(c1, c2, c3, c4, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4)

          case ZioSchema.Enum5(c1, c2, c3, c4, c5, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5)

          case ZioSchema.Enum6(c1, c2, c3, c4, c5, c6, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6)

          case ZioSchema.Enum7(c1, c2, c3, c4, c5, c6, c7, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7)

          case ZioSchema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8)

          case ZioSchema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)

          case ZioSchema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

          case ZioSchema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)

          case ZioSchema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)

          case ZioSchema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)

          case ZioSchema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)

          case ZioSchema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)

          case ZioSchema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)

          case ZioSchema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)

          case ZioSchema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)

          case ZioSchema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)

          case ZioSchema.Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)

          case ZioSchema.Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)

          case ZioSchema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)

          case _ =>
            ???
        }
        //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
      }

      private def convertEnum[A](
        annotations: Chunk[Any],
        cases: ZioSchema.Case[_, A]*
      ): Decoder[A] = {
        val enumAnnotations = extractAnnotations[A](annotations)
        val decodersByName = cases.iterator
          .map { c =>
            val cAnn = extractAnnotations(c.annotations)
            val decoder = addAnnotations(
              convert(c.codec),
              extractAnnotations(c.annotations)
            ).asInstanceOf[io.circe.Decoder[Any]]
            val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
              s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive an io.circe.Decoder. Received annotations: $cAnn"
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

      private def genericRecordConverter(fieldSet: FieldSet, annotations: Chunk[Any]): Decoder[ListMap[String, _]] = {
        val parsed = extractAnnotations[ListMap[String, _]](annotations)
        convertUsingCache(parsed) {
          val fieldDecoders = fieldSet.toChunk.iterator
            .map { field =>
              val fieldDecoder = addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
              parsed.transformJsonLabel(field.label) -> (field.label, fieldDecoder)
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
          case StandardType.UnitType => io.circe.Decoder[Unit]
          case StandardType.StringType => io.circe.Decoder[String]
          case StandardType.BoolType => io.circe.Decoder[Boolean]
          case StandardType.ShortType => io.circe.Decoder[Short]
          case StandardType.IntType => io.circe.Decoder[Int]
          case StandardType.LongType => io.circe.Decoder[Long]
          case StandardType.FloatType => io.circe.Decoder[Float]
          case StandardType.DoubleType => io.circe.Decoder[Double]
          case StandardType.BinaryType => ???
          case StandardType.CharType => io.circe.Decoder[String]
          case StandardType.BigIntegerType => io.circe.Decoder[BigInteger]
          case StandardType.BigDecimalType => io.circe.Decoder[BigDecimal]
          case StandardType.UUIDType => io.circe.Decoder[UUID]
          case StandardType.DayOfWeekType => io.circe.Decoder[Int] // todo add validation
          case StandardType.DurationType => io.circe.Decoder[String]
          case StandardType.InstantType(_) => io.circe.Decoder[Instant]
          case StandardType.LocalDateType(_) => io.circe.Decoder[LocalDate]
          case StandardType.LocalDateTimeType(_) => io.circe.Decoder[LocalDateTime]
          case StandardType.LocalTimeType(_) => io.circe.Decoder[LocalTime]
          case StandardType.MonthType => io.circe.Decoder[String] // todo add validation
          case StandardType.MonthDayType => io.circe.Decoder[String] // todo add validation
          case StandardType.OffsetDateTimeType(_) => io.circe.Decoder[OffsetDateTime]
          case StandardType.OffsetTimeType(_) => io.circe.Decoder[OffsetTime]
          case StandardType.PeriodType => io.circe.Decoder[Period]
          case StandardType.YearType => io.circe.Decoder[Year]
          case StandardType.YearMonthType => io.circe.Decoder[YearMonth]
          case StandardType.ZonedDateTimeType(_) => io.circe.Decoder[ZonedDateTime]
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
            validator(a).map(validationErrorMessage)
          }
        }
        decoder
      }

      private def validationErrorMessage(validationError: ValidationError[_]): String = {
        validationError match {
          case primitive: ValidationError.Primitive[_] =>
            primitive.validator match {
              case v: Validator.Min[_] =>
                s"Value must be greater${if (v.exclusive) "" else " or equal"} than ${v.value}. Received: ${validationError.invalidValue}."
              case v: Validator.Max[_] =>
                s"Value must be smaller${if (v.exclusive) "" else " or equal"} than ${v.value}. Received: ${validationError.invalidValue}."
              case pattern: Validator.Pattern[_] =>
                s"Value must match the pattern: ${pattern.value}. Received: '${validationError.invalidValue}'."
              case v: Validator.MinLength[_] =>
                val value = validationError.invalidValue.toString
                if (value.isEmpty) {
                  s"Length of the value must be greater or equal than ${v.value}. Received empty value."
                }
                else {
                  s"Length of the value must be greater or equal than ${v.value}. Received: '${validationError.invalidValue}'."
                }
              case v: Validator.MaxLength[_] =>
                val value = validationError.invalidValue.toString
                val truncated = value.take(v.value + 1)
                val charsLeft = value.length - truncated.length
                val formatted =
                  if (charsLeft <= 0) value
                  else s"""$truncated[truncated to ${truncated.length}](+$charsLeft more)""""
                s"Length of the value must be smaller or equal than ${v.value}. Received: '$formatted'."
              case v: Validator.MinSize[_, _] =>
                s"Size of the provided array must be greater or equal than ${v.value}. Received array of size ${validationError.invalidValue.asInstanceOf[Iterable[_]].size}."
              case v: Validator.MaxSize[_, _] =>
                s"Size of the provided array must be smaller or equal than ${v.value}. Received array of size ${validationError.invalidValue.asInstanceOf[Iterable[_]].size}."
              case e: Validator.Enumeration[_] =>
                s"Value must be one of: ${e.possibleValues.mkString(", ")}. Received: '${validationError.invalidValue}'."
            }
          case custom: ValidationError.Custom[_] =>
            custom.message
        }
      }
    }
  }

  object Encoder {
    def convert[A](zioSchema: ZioSchema[A]): Encoder[A] = {
      new Converter(scala.collection.mutable.Map.empty).convert(zioSchema)
    }

    final private[openapi] class LazyEncoder[A]() extends io.circe.Encoder[A] {
      private var _encoder: io.circe.Encoder[A] = _
      private[Encoder] def set(encoder: io.circe.Encoder[A]): Unit =
        this._encoder = encoder
      private def get: io.circe.Encoder[A] =
        if (_encoder == null) throw new RuntimeException("LazyEncoder has not yet been initialized")
        else _encoder
      override def apply(a: A): Json = get(a)
    }

    private class Converter(cache: scala.collection.mutable.Map[CacheKey, LazyEncoder[_]]) {

      private def convertUsingCache[A](annotations: OpenApiParsedAnnotations[A])(convert: => io.circe.Encoder[A])
        : io.circe.Encoder[A] = {
        annotations.entityName match {
          case Some(name) =>
            val cacheKey = CacheKey(name, annotations.hashCode())
            cache.get(cacheKey) match {
              case Some(value) => value.asInstanceOf[io.circe.Encoder[A]]
              case None =>
                val lazyEnc = new LazyEncoder[A]()
                val _ = cache.addOne(cacheKey -> lazyEnc)
                val result = convert
                lazyEnc.set(result)
                result
            }
          case None =>
            convert
        }
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

          case ZioSchema.MapSchema(_, _, _) =>
            ???

          case ZioSchema.SetSchema(schema, annotation) =>
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

          case ZioSchema.Tuple(_, _, _) =>
            ???

          case ZioSchema.Optional(schema, annotations) =>
            addAnnotations[A](
              baseEncoder = io.circe.Encoder.encodeOption(convert(schema)).asInstanceOf[Encoder[A]],
              metadata = extractAnnotations(annotations)
            )

          case ZioSchema.Fail(_, _) =>
            ???

          case ZioSchema.GenericRecord(fieldSet, annotations) =>
            val recordAnnotations: OpenApiParsedAnnotations[A] = extractAnnotations[A](annotations)
            convertUsingCache(recordAnnotations) {
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

          case either @ ZioSchema.EitherSchema(_, _, _) =>
            convert(either.toEnum).asInstanceOf[Encoder[A]]

          case l @ ZioSchema.Lazy(_) =>
            convert(l.schema)

          case ZioSchema.Meta(_, _) =>
            ???

          case ZioSchema.CaseClass1(f1, _, ext1, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val baseEncoder = io.circe.Encoder.forProduct1(parsed.transformJsonLabel(f1.label))(ext1)(encoder1)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass2(f1, f2, _, ext1, ext2, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val baseEncoder = io.circe.Encoder.forProduct2(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label)
              )((a: A) => (ext1(a), ext2(a)))(encoder1, encoder2)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass3(f1, f2, f3, _, ext1, ext2, ext3, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val baseEncoder = io.circe.Encoder.forProduct3(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a)))(encoder1, encoder2, encoder3)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass4(f1, f2, f3, f4, _, ext1, ext2, ext3, ext4, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val baseEncoder = io.circe.Encoder.forProduct4(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a)))(encoder1, encoder2, encoder3, encoder4)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass5(f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val baseEncoder = io.circe.Encoder.forProduct5(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a)))(encoder1, encoder2, encoder3, encoder4, encoder5)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass6(f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val baseEncoder = io.circe.Encoder.forProduct6(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val baseEncoder = io.circe.Encoder.forProduct7(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val baseEncoder = io.circe.Encoder.forProduct8(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val baseEncoder = io.circe.Encoder.forProduct9(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val baseEncoder = io.circe.Encoder.forProduct10(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val baseEncoder = io.circe.Encoder.forProduct11(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val baseEncoder = io.circe.Encoder.forProduct12(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val baseEncoder = io.circe.Encoder.forProduct13(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val baseEncoder = io.circe.Encoder.forProduct14(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val baseEncoder = io.circe.Encoder.forProduct15(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val baseEncoder = io.circe.Encoder.forProduct16(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val baseEncoder = io.circe.Encoder.forProduct17(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val encoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val baseEncoder = io.circe.Encoder.forProduct18(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a), ext18(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val encoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val encoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val baseEncoder = io.circe.Encoder.forProduct19(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a), ext18(a), ext19(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val encoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val encoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val encoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val baseEncoder = io.circe.Encoder.forProduct20(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a), ext18(a), ext19(a), ext20(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val encoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val encoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val encoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val encoder21 = addAnnotations(convert(f21.schema), extractAnnotations(f21.annotations))
              val baseEncoder = io.circe.Encoder.forProduct21(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label),
                parsed.transformJsonLabel(f21.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a), ext18(a), ext19(a), ext20(a), ext21(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20, encoder21)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22, annotations) =>
            val parsed = extractAnnotations[A](annotations)
            convertUsingCache(parsed) {
              val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
              val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
              val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
              val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
              val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
              val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
              val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
              val encoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
              val encoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
              val encoder10 = addAnnotations(convert(f10.schema), extractAnnotations(f10.annotations))
              val encoder11 = addAnnotations(convert(f11.schema), extractAnnotations(f11.annotations))
              val encoder12 = addAnnotations(convert(f12.schema), extractAnnotations(f12.annotations))
              val encoder13 = addAnnotations(convert(f13.schema), extractAnnotations(f13.annotations))
              val encoder14 = addAnnotations(convert(f14.schema), extractAnnotations(f14.annotations))
              val encoder15 = addAnnotations(convert(f15.schema), extractAnnotations(f15.annotations))
              val encoder16 = addAnnotations(convert(f16.schema), extractAnnotations(f16.annotations))
              val encoder17 = addAnnotations(convert(f17.schema), extractAnnotations(f17.annotations))
              val encoder18 = addAnnotations(convert(f18.schema), extractAnnotations(f18.annotations))
              val encoder19 = addAnnotations(convert(f19.schema), extractAnnotations(f19.annotations))
              val encoder20 = addAnnotations(convert(f20.schema), extractAnnotations(f20.annotations))
              val encoder21 = addAnnotations(convert(f21.schema), extractAnnotations(f21.annotations))
              val encoder22 = addAnnotations(convert(f22.schema), extractAnnotations(f22.annotations))
              val baseEncoder = io.circe.Encoder.forProduct22(
                parsed.transformJsonLabel(f1.label),
                parsed.transformJsonLabel(f2.label),
                parsed.transformJsonLabel(f3.label),
                parsed.transformJsonLabel(f4.label),
                parsed.transformJsonLabel(f5.label),
                parsed.transformJsonLabel(f6.label),
                parsed.transformJsonLabel(f7.label),
                parsed.transformJsonLabel(f8.label),
                parsed.transformJsonLabel(f9.label),
                parsed.transformJsonLabel(f10.label),
                parsed.transformJsonLabel(f11.label),
                parsed.transformJsonLabel(f12.label),
                parsed.transformJsonLabel(f13.label),
                parsed.transformJsonLabel(f14.label),
                parsed.transformJsonLabel(f15.label),
                parsed.transformJsonLabel(f16.label),
                parsed.transformJsonLabel(f17.label),
                parsed.transformJsonLabel(f18.label),
                parsed.transformJsonLabel(f19.label),
                parsed.transformJsonLabel(f20.label),
                parsed.transformJsonLabel(f21.label),
                parsed.transformJsonLabel(f22.label)
              )((a: A) => (ext1(a), ext2(a), ext3(a), ext4(a), ext5(a), ext6(a), ext7(a), ext8(a), ext9(a), ext10(a), ext11(a), ext12(a), ext13(a), ext14(a), ext15(a), ext16(a), ext17(a), ext18(a), ext19(a), ext20(a), ext21(a), ext22(a)))(encoder1, encoder2, encoder3, encoder4, encoder5, encoder6, encoder7, encoder8, encoder9, encoder10, encoder11, encoder12, encoder13, encoder14, encoder15, encoder16, encoder17, encoder18, encoder19, encoder20, encoder21, encoder22)
              addAnnotations(baseEncoder, parsed)
            }

          case ZioSchema.Enum1(c1, annotations) =>
            convertEnum[A](annotations, c1)

          case ZioSchema.Enum2(c1, c2, annotations) =>
            convertEnum[A](annotations, c1, c2)

          case ZioSchema.Enum3(c1, c2, c3, annotations) =>
            convertEnum[A](annotations, c1, c2, c3)

          case ZioSchema.Enum4(c1, c2, c3, c4, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4)

          case ZioSchema.Enum5(c1, c2, c3, c4, c5, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5)

          case ZioSchema.Enum6(c1, c2, c3, c4, c5, c6, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6)

          case ZioSchema.Enum7(c1, c2, c3, c4, c5, c6, c7, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7)

          case ZioSchema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8)

          case ZioSchema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)

          case ZioSchema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

          case ZioSchema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)

          case ZioSchema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)

          case ZioSchema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)

          case ZioSchema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)

          case ZioSchema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)

          case ZioSchema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)

          case ZioSchema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)

          case ZioSchema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)

          case ZioSchema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)

          case ZioSchema.Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)

          case ZioSchema.Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)

          case ZioSchema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
            convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)

          // e.g. enums are not yet supported
          case _ =>
            ???
        }
        //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
      }

      private def convertEnum[A](
        annotations: Chunk[Any],
        cases: ZioSchema.Case[_, A]*
      ): Encoder[A] = {
        val enumAnnotations = extractAnnotations[A](annotations)
        val encodersByName = cases.iterator
          .map { c =>
            val cAnn = extractAnnotations(c.annotations)
            val encoder = addAnnotations(
              convert(c.codec),
              extractAnnotations(c.annotations)
            ).asInstanceOf[io.circe.Encoder[Any]]
            val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
              s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive an io.circe.Encoder. Received annotations: $cAnn"
            ))
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
                val json = enc(c.unsafeDeconstruct(a).asInstanceOf[Any])
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
          case StandardType.ShortType => io.circe.Encoder[Short]
          case StandardType.IntType => io.circe.Encoder[Int]
          case StandardType.LongType => io.circe.Encoder[Long]
          case StandardType.FloatType => io.circe.Encoder[Float]
          case StandardType.DoubleType => io.circe.Encoder[Double]
          case StandardType.BinaryType => ???
          case StandardType.CharType => io.circe.Encoder[String]
          case StandardType.BigIntegerType => io.circe.Encoder[BigInteger]
          case StandardType.BigDecimalType => io.circe.Encoder[BigDecimal]
          case StandardType.UUIDType => io.circe.Encoder[UUID]
          case StandardType.DayOfWeekType => io.circe.Encoder[Int] // todo add validation
          case StandardType.DurationType => io.circe.Encoder[String]
          case StandardType.InstantType(_) => io.circe.Encoder[Instant]
          case StandardType.LocalDateType(_) => io.circe.Encoder[LocalDate]
          case StandardType.LocalDateTimeType(_) => io.circe.Encoder[LocalDateTime]
          case StandardType.LocalTimeType(_) => io.circe.Encoder[LocalTime]
          case StandardType.MonthType => io.circe.Encoder[String] // todo add validation
          case StandardType.MonthDayType => io.circe.Encoder[String] // todo add validation
          case StandardType.OffsetDateTimeType(_) => io.circe.Encoder[OffsetDateTime]
          case StandardType.OffsetTimeType(_) => io.circe.Encoder[OffsetTime]
          case StandardType.PeriodType => io.circe.Encoder[Period]
          case StandardType.YearType => io.circe.Encoder[Year]
          case StandardType.YearMonthType => io.circe.Encoder[YearMonth]
          case StandardType.ZonedDateTimeType(_) => io.circe.Encoder[ZonedDateTime]
          case StandardType.ZoneIdType => io.circe.Encoder[ZoneId]
          case StandardType.ZoneOffsetType => io.circe.Encoder[ZoneOffset]
        }
        addAnnotations(baseEncoder.asInstanceOf[Encoder[A]], extractAnnotations(annotations))
      }

      @nowarn
      private def addAnnotations[A](
        baseEncoder: Encoder[A],
        metadata: OpenApiParsedAnnotations[A]
      ): Encoder[A] = {
        baseEncoder
      }
    }
  }
}
