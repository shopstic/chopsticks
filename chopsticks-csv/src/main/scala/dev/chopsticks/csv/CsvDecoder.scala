package dev.chopsticks.csv

import dev.chopsticks.openapi.{OpenApiParsedAnnotations, OpenApiSumTypeSerDeStrategy, OpenApiValidation}
import dev.chopsticks.openapi.common.{ConverterCache, OpenApiConverterUtils}
import sttp.tapir.Validator
import zio.schema.{FieldSet, Schema, StandardType, TypeId}
import zio.Chunk
import zio.schema.Schema.Primitive

import java.time.{
  DayOfWeek,
  Duration,
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.UUID
import scala.collection.compat.immutable.ArraySeq
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

trait CsvDecoder[A] { self =>
  def isOptional: Boolean
  def isPrimitive: Boolean
  def parse(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], A]
  def parseAsOption(
    row: Map[String, String],
    columnName: Option[String]
  ): Either[List[CsvDecoderError], Option[A]]
  final def ensure(pred: A => Boolean, message: => String, options: CsvCodecOptions) =
    new CsvDecoder[A] {
      override val isOptional = self.isOptional
      override val isPrimitive = self.isPrimitive
      override def parseAsOption(row: Map[String, String], columnName: Option[String]) = {
        self.parseAsOption(row, columnName) match {
          case r @ Right(Some(value)) =>
            if (pred(value)) r else Left(List(CsvDecoderError(message, errorColumn(options, columnName))))
          case r @ Right(None) => r
          case l @ Left(_) => l
        }
      }
      override def parse(row: Map[String, String], column: Option[String]): Either[List[CsvDecoderError], A] =
        self.parse(row, column) match {
          case r @ Right(value) =>
            if (pred(value)) r
            else Left(List(CsvDecoderError(message, errorColumn(options, column))))
          case l @ Left(_) => l
        }
    }
  final def ensure(errors: A => List[String], options: CsvCodecOptions) = new CsvDecoder[A] {
    override val isOptional = self.isOptional
    override val isPrimitive = self.isPrimitive
    override def parseAsOption(
      row: Map[String, String],
      columnName: Option[String]
    ): Either[List[CsvDecoderError], Option[A]] = {
      self.parseAsOption(row, columnName) match {
        case r @ Right(Some(value)) =>
          errors(value) match {
            case Nil => r
            case errs =>
              Left(errs.map(e => CsvDecoderError(e, errorColumn(options, columnName))))
          }
        case r @ Right(None) => r
        case l @ Left(_) => l
      }
    }
    override def parse(row: Map[String, String], column: Option[String]): Either[List[CsvDecoderError], A] =
      self.parse(row, column) match {
        case r @ Right(value) =>
          errors(value) match {
            case Nil => r
            case errs =>
              Left(errs.map(e => CsvDecoderError(e, errorColumn(options, column))))
          }
        case l @ Left(_) => l
      }
  }
  final def map[B](f: A => B): CsvDecoder[B] = new CsvDecoder[B] {
    override val isOptional = self.isOptional
    override val isPrimitive = self.isPrimitive
    override def parseAsOption(
      row: Map[String, String],
      columnName: Option[String]
    ): Either[List[CsvDecoderError], Option[B]] = {
      self.parseAsOption(row, columnName).map(_.map(f))
    }
    override def parse(row: Map[String, String], column: Option[String]): Either[List[CsvDecoderError], B] = {
      self.parse(row, column).map(f)
    }
  }
  final def emap[B](f: A => Either[String, B]): CsvDecoder[B] = new CsvDecoder[B] {
    override val isOptional = self.isOptional
    override val isPrimitive = self.isPrimitive
    override def parseAsOption(
      row: Map[String, String],
      columnName: Option[String]
    ): Either[List[CsvDecoderError], Option[B]] = {
      self.parseAsOption(row, columnName) match {
        case Right(Some(value)) =>
          f(value) match {
            case Right(v) => Right(Some(v))
            case Left(error) => Left(List(CsvDecoderError(error, columnName)))
          }
        case r @ Right(None) => r.asInstanceOf[Either[List[CsvDecoderError], Option[B]]]
        case Left(value) => Left(value)
      }
    }
    override def parse(row: Map[String, String], column: Option[String]): Either[List[CsvDecoderError], B] = {
      self.parse(row, column) match {
        case Right(value) =>
          f(value) match {
            case r @ Right(_) => r.asInstanceOf[Either[List[CsvDecoderError], B]]
            case Left(error) => Left(List(CsvDecoderError(error, column)))
          }
        case Left(value) => Left(value)
      }
    }
  }

  protected def errorColumn(
    options: CsvCodecOptions,
    columnName: Option[String]
  ): Option[String] = {
    CsvDecoder.errorColumn(columnName, this.isPrimitive, options)
  }
}

object CsvDecoder extends CsvProductDecoders {
  import dev.chopsticks.openapi.OpenApiParsedAnnotations._

  def derive[A](options: CsvCodecOptions = CsvCodecOptions.default)(implicit
    schema: Schema[A]
  ): CsvDecoder[A] = {
    new Converter(options).convert(schema)
  }

  private[csv] def errorColumn(
    columnName: Option[String],
    isPrimitive: Boolean,
    options: CsvCodecOptions
  ): Option[String] = {
    if (isPrimitive) columnName
    else Some(options.nestedFieldLabel(columnName, "*"))
  }

  def caseClass0Decoder[A](construct: () => A): CsvDecoder[A] = new CsvDecoder[A] {
    override val isOptional = false
    override val isPrimitive = false
    override def parseAsOption(
      row: Map[String, String],
      columnName: Option[String]
    ): Either[List[CsvDecoderError], Option[A]] = {
      Right(Some(construct()))
    }
    override def parse(
      value: Map[String, String],
      column: Option[String]
    ): Either[List[CsvDecoderError], A] = {
      Right(construct())
    }
  }

  private val unitDecoder: CsvDecoder[Unit] = createPrimitive {
    case (v, _) if v.isBlank => Right(())
    case (_, col) => Left(List(CsvDecoderError(s"Cannot parse value to Unit type.", col)))
  }

  private val boolDecoder: CsvDecoder[Boolean] = createPrimitive { case (value, col) =>
    value.toBooleanOption match {
      case Some(n) => Right(n)
      case None =>
        Left(List(CsvDecoderError(s"Cannot parse boolean (it must be either 'true' or 'false').", col)))
    }
  }

  private val byteDecoder: CsvDecoder[Byte] = createPrimitive { case (value, col) =>
    value.toByteOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse byte.", col)))
    }
  }

  private val shortDecoder: CsvDecoder[Short] = createPrimitive { case (value, col) =>
    value.toShortOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val intDecoder: CsvDecoder[Int] = createPrimitive { case (value, col) =>
    value.toIntOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val longDecoder: CsvDecoder[Long] = createPrimitive { case (value, col) =>
    value.toLongOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val floatDecoder: CsvDecoder[Float] = createPrimitive { case (value, col) =>
    value.toFloatOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val doubleDecoder: CsvDecoder[Double] = createPrimitive { case (value, col) =>
    value.toDoubleOption match {
      case Some(n) => Right(n)
      case None => Left(List(CsvDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val charDecoder: CsvDecoder[Char] = createPrimitive { case (value, col) =>
    if (value.length == 1) Right(value.charAt(0))
    else Left(List(CsvDecoderError(s"Cannot parse char. Got string with length ${value.length} instead.", col)))
  }

  private val stringDecoder: CsvDecoder[String] = createPrimitivePure(identity)

  private val instantDecoder: CsvDecoder[Instant] = {
    createFromThrowing(Instant.parse, s"Cannot parse timestamp; it must be in ISO-8601 format.")
  }

  private val dayOfWeekDecoder: CsvDecoder[DayOfWeek] = {
    createFromThrowing(
      DayOfWeek.valueOf, {
        val expected = DayOfWeek.values().iterator.map(_.toString).mkString(", ")
        s"Cannot parse day of week. Expected one of: $expected."
      }
    )
  }

  private val durationDecoder: CsvDecoder[Duration] = {
    createFromThrowing(
      Duration.parse,
      s"Cannot parse duration; it must be in ISO-8601 format."
    )
  }

  private val localTimeDecoder: CsvDecoder[LocalTime] = {
    createFromThrowing(
      LocalTime.parse,
      s"""Cannot parse local time; expected text such as "10:15:00"."""
    )
  }

  private val localDateTimeDecoder: CsvDecoder[LocalDateTime] = {
    createFromThrowing(
      LocalDateTime.parse,
      s"""Cannot parse local date time; expected text such as "2020-12-03T10:15:30"."""
    )
  }

  private val offsetTimeDecoder: CsvDecoder[OffsetTime] = {
    createFromThrowing(
      OffsetTime.parse,
      s"""Cannot parse offset time; expected text such as "10:15:30+01:00"."""
    )
  }

  private val offsetDateTimeDecoder: CsvDecoder[OffsetDateTime] = {
    createFromThrowing(
      OffsetDateTime.parse,
      s"""Cannot parse offset date time; expected text such as "2020-12-03T10:15:30+01:00"."""
    )
  }

  private val zonedDateTimeDecoder: CsvDecoder[ZonedDateTime] = {
    createFromThrowing(
      ZonedDateTime.parse,
      s"""Cannot parse zoned date time; expected text such as "2020-12-03T10:15:30+01:00[Europe/Paris]"."""
    )
  }

  private val bigDecimalDecoder: CsvDecoder[java.math.BigDecimal] = {
    createFromThrowing(BigDecimal.apply(_).underlying(), s"Cannot parse BigDecimal number.")
  }

  private val bigIntDecoder: CsvDecoder[java.math.BigInteger] = {
    createFromThrowing(BigInt.apply(_).underlying(), s"Cannot parse BigInteger number.")
  }

  private val localDateDecoder: CsvDecoder[LocalDate] = {
    createFromThrowing(LocalDate.parse, s"Cannot parse date; it must be in ISO-8601 format (i.e. 'yyyy-MM-dd').")
  }

  private val uuidDecoder: CsvDecoder[UUID] = {
    createFromThrowing(UUID.fromString, s"Cannot parse UUID.")
  }

  private val zoneIdDecoder: CsvDecoder[ZoneId] = {
    createFromThrowing(str => ZoneId.of(str), s"Cannot parse ZoneId.")
  }

  private val zoneOffsetDecoder: CsvDecoder[ZoneOffset] = {
    createFromThrowing(str => ZoneOffset.of(str), s"Cannot parse ZoneOffset.")
  }

  private def decodeOption[A](d: CsvDecoder[A]): CsvDecoder[Option[A]] =
    new CsvDecoder[Option[A]] {
      override val isOptional = true
      override val isPrimitive = d.isPrimitive
      override def parseAsOption(
        row: Map[String, String],
        columnName: Option[String]
      ): Either[List[CsvDecoderError], Option[Option[A]]] = {
        parse(row, columnName).map(maybeA => maybeA.map(Some(_)))
      }
      override def parse(
        row: Map[String, String],
        column: Option[String]
      ): Either[List[CsvDecoderError], Option[A]] = {
        d.parseAsOption(row, column)
      }
    }

  private def decodeChunkWithPositionSuffix[A](
    options: CsvCodecOptions,
    d: CsvDecoder[A],
    maxElems: Int
  ): CsvDecoder[Chunk[A]] =
    new CsvDecoder[Chunk[A]] {
      override val isOptional = false
      override val isPrimitive = false
      override def parseAsOption(
        row: Map[String, String],
        columnName: Option[String]
      ): Either[List[CsvDecoderError], Option[Chunk[A]]] = {
        parseChunk(row, columnName, parseFirstElem = false).map { chunk =>
          if (chunk.isEmpty) None // columns were not defined
          else Some(chunk)
        }
      }
      override def parse(
        row: Map[String, String],
        column: Option[String]
      ): Either[List[CsvDecoderError], Chunk[A]] = {
        parseChunk(row, column, parseFirstElem = true)
      }

      private def parseChunk(
        row: Map[String, String],
        column: Option[String],
        parseFirstElem: Boolean
      ): Either[List[CsvDecoderError], Chunk[A]] = {
        val seq = zio.ChunkBuilder.make[A](maxElems)
        val errors = List.newBuilder[CsvDecoderError]

        var i = 0
        var continue = true
        while (i < maxElems && continue) {
          val colName = options.nestedArrayFieldLabel(column, i)
          if (i == 0 && parseFirstElem) {
            d.parse(row, Some(colName)) match {
              case Right(value) => val _ = seq += value
              case Left(e) => val _ = errors ++= e
            }
          }
          else {
            d.parseAsOption(row, Some(colName)) match {
              case Right(Some(value)) =>
                val _ = seq += value
              case Right(None) =>
                continue = false
              case Left(e) =>
                val _ = errors ++= e
            }
          }
          i += 1
        }

        val errs = errors.result()
        if (errs.isEmpty) Right(seq.result())
        else Left(errs)
      }
    }

  private def createPrimitivePure[A](f: String => A) = createPrimitive { case (value, _) => Right(f(value)) }

  private def createFromThrowing[A](f: String => A, errorMessage: => String) = createPrimitive {
    case (value, maybeColumn) =>
      try Right(f(value))
      catch { case NonFatal(_) => Left(List(CsvDecoderError(errorMessage, maybeColumn))) }
  }

  private def createPrimitive[A](f: (String, Option[String]) => Either[List[CsvDecoderError], A]) =
    new CsvDecoder[A] {
      override val isOptional = false
      override val isPrimitive = true
      override def parseAsOption(
        row: Map[String, String],
        columnName: Option[String]
      ): Either[List[CsvDecoderError], Option[A]] = {
        columnName match {
          case Some(c) =>
            row.get(c) match {
              case Some(v) if v.nonEmpty => f(v, columnName).map(r => Some(r))
              case _ => Right(None)
            }
          case None => throw columnNotProvided
        }
      }
      override def parse(
        value: Map[String, String],
        column: Option[String]
      ): Either[List[CsvDecoderError], A] = {
        column match {
          case Some(c) =>
            value.get(c) match {
              case Some(v) => f(v, column)
              case None => Left(List(CsvDecoderError.columnNotExists(c)))
            }
          case None => throw columnNotProvided
        }
      }
    }

  private def columnNotProvided =
    new RuntimeException(s"Error in CsvDecoder, column was not provided for a primitive value.")

  final private[csv] class LazyDecoder[A] extends CsvDecoder[A] with ConverterCache.Lazy[CsvDecoder[A]] {
    override def isOptional: Boolean = get.isOptional
    override def isPrimitive: Boolean = get.isOptional
    override def parse(row: Map[String, String], columnName: Option[String]): Either[List[CsvDecoderError], A] =
      get.parse(row, columnName)
    override def parseAsOption(
      row: Map[String, String],
      columnName: Option[String]
    ): Either[List[CsvDecoderError], Option[A]] =
      get.parseAsOption(row, columnName)
  }

  private class Converter(
    options: CsvCodecOptions,
    cache: ConverterCache[CsvDecoder] = new ConverterCache[CsvDecoder]()
  ) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: OpenApiParsedAnnotations[A]
    )(convert: => CsvDecoder[A]): CsvDecoder[A] = {
      cache.convertUsingCache(typeId, annotations)(convert)(() => new LazyDecoder[A]())
    }

    // scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
    def convert[A](schema: Schema[A] /*, modifiers: Any => Any*/ ): CsvDecoder[A] =
      schema match {
        case Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, fromChunk, _, annotations, _) =>
          addAnnotations(
            decodeChunkWithPositionSuffix(options, convert(schemaA), maxElems = options.maxSeqSize).map(fromChunk),
            extractAnnotations(annotations)
          )

        case Schema.Set(_, _) =>
          ???

        case Schema.Transform(schema, f, _, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseDecoder = convert(schema).emap(f)
          addAnnotations(baseDecoder, typedAnnotations)

        case Schema.Optional(schema, annotations) =>
          addAnnotations[A](
            baseDecoder = decodeOption(convert(schema)),
            metadata = extractAnnotations(annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema)

        case Schema.GenericRecord(id, fieldSet, annotations) =>
          genericRecordConverter(id, fieldSet, annotations)

        case Schema.CaseClass0(id, construct, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(id, parsed) {
            val baseDecoder = caseClass0Decoder(construct)
            addAnnotations(baseDecoder, parsed)
          }

        case Schema.CaseClass1(id, f1, construct, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(id, parsed) {
            val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val baseDecoder = forProduct1(
              options = options,
              names = ArraySeq(f1.name.toString),
              decoders = ArraySeq(decoder1)
            )(construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass2(_, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val baseDecoder = forProduct2(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass3(_, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val baseDecoder = forProduct3(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass4(_, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val baseDecoder = forProduct4(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass5(_, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
            val baseDecoder = forProduct5(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass6(_, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
            val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
            val baseDecoder = forProduct6(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass7(_, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
            val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
            val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
            val baseDecoder = forProduct7(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
            val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
            val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
            val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
            val baseDecoder = forProduct8(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
            val decoder1 = addAnnotations(convert(s.field1.schema), extractAnnotations(s.field1.annotations))
            val decoder2 = addAnnotations(convert(s.field2.schema), extractAnnotations(s.field2.annotations))
            val decoder3 = addAnnotations(convert(s.field3.schema), extractAnnotations(s.field3.annotations))
            val decoder4 = addAnnotations(convert(s.field4.schema), extractAnnotations(s.field4.annotations))
            val decoder5 = addAnnotations(convert(s.field5.schema), extractAnnotations(s.field5.annotations))
            val decoder6 = addAnnotations(convert(s.field6.schema), extractAnnotations(s.field6.annotations))
            val decoder7 = addAnnotations(convert(s.field7.schema), extractAnnotations(s.field7.annotations))
            val decoder8 = addAnnotations(convert(s.field8.schema), extractAnnotations(s.field8.annotations))
            val decoder9 = addAnnotations(convert(s.field9.schema), extractAnnotations(s.field9.annotations))
            val baseDecoder = forProduct9(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct10(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct11(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct12(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct13(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct14(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct15(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct16(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct17(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct18(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString,
                s.field18.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct19(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString,
                s.field18.name.toString,
                s.field19.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct20(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString,
                s.field18.name.toString,
                s.field19.name.toString,
                s.field20.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct21(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString,
                s.field18.name.toString,
                s.field19.name.toString,
                s.field20.name.toString,
                s.field21.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s @ Schema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val parsed = extractAnnotations[A](s.annotations)
          convertUsingCache(s.id, parsed) {
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
            val baseDecoder = forProduct22(
              options = options,
              names = ArraySeq(
                s.field1.name.toString,
                s.field2.name.toString,
                s.field3.name.toString,
                s.field4.name.toString,
                s.field5.name.toString,
                s.field6.name.toString,
                s.field7.name.toString,
                s.field8.name.toString,
                s.field9.name.toString,
                s.field10.name.toString,
                s.field11.name.toString,
                s.field12.name.toString,
                s.field13.name.toString,
                s.field14.name.toString,
                s.field15.name.toString,
                s.field16.name.toString,
                s.field17.name.toString,
                s.field18.name.toString,
                s.field19.name.toString,
                s.field20.name.toString,
                s.field21.name.toString,
                s.field22.name.toString
              ),
              decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21, decoder22)
            )(s.construct)
            addAnnotations(baseDecoder, parsed)
          }

        case s: Schema.Enum[A] =>
          convertEnum[A](s.annotations, s.cases)

        case _ =>
          ???
      }

    // scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

    private def convertEnum[A](
      annotations: Chunk[Any],
      cases: Chunk[Schema.Case[A, _]]
    ): CsvDecoder[A] = {
      val enumAnnotations = extractAnnotations[A](annotations)
      val decodersByName = cases.iterator
        .map { c =>
          val cAnn = extractAnnotations(c.annotations)
          val decoder = addAnnotations(
            convert(c.schema),
            extractAnnotations(c.annotations)
          ).asInstanceOf[CsvDecoder[Any]]
          val entityName = OpenApiConverterUtils.getCaseEntityName(c, cAnn).getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined or be a case class to derive a CsvDecoder. Received annotations: $cAnn"
          ))
          entityName -> decoder
        }
        .toMap
      val discriminator = enumAnnotations.sumTypeSerDeStrategy

      val decoder = discriminator
        .getOrElse(throw new RuntimeException(
          s"Discriminator must be defined to derive an CsvDecoder. Received annotations: $enumAnnotations"
        )) match {
        case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
          val diff = discriminator.mapping.values.toSet.diff(decodersByName.keySet)
          if (diff.nonEmpty) {
            throw new RuntimeException(
              s"Cannot derive CsvDecoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and decoders don't match. Diff=$diff."
            )
          }
          new CsvDecoder[A] {
            private val knownObjectTypes = discriminator.mapping.keys.toList.sorted.mkString(", ")
            override val isOptional = false
            override val isPrimitive = false

            override def parse(
              row: Map[String, String],
              columnName: Option[String]
            ): Either[List[CsvDecoderError], A] = {
              val discriminatorColumnName =
                Some(options.nestedFieldLabel(columnName, discriminator.discriminatorFieldName))
              val eitherType = stringDecoder.parse(row, discriminatorColumnName)
              eitherType match {
                case Left(errors) => Left(errors)
                case Right(tpe) =>
                  discriminator.mapping.get(tpe) match {
                    case None =>
                      Left(List(CsvDecoderError.unrecognizedDiscriminatorType(tpe, knownObjectTypes, columnName)))
                    case Some(value) =>
                      decodersByName(value).parse(row, columnName).asInstanceOf[Either[List[CsvDecoderError], A]]
                  }
              }
            }

            override def parseAsOption(
              row: Map[String, String],
              columnName: Option[String]
            ): Either[List[CsvDecoderError], Option[A]] = {
              val discriminatorColumnName =
                Some(options.nestedFieldLabel(columnName, discriminator.discriminatorFieldName))
              val eitherOptionType = stringDecoder.parseAsOption(row, discriminatorColumnName)
              eitherOptionType match {
                case Left(errors) => Left(errors)
                case Right(maybeType) =>
                  maybeType match {
                    case None => Right(None)
                    case Some(tpe) =>
                      discriminator.mapping.get(tpe) match {
                        case None =>
                          Left(List(CsvDecoderError.unrecognizedDiscriminatorType(tpe, knownObjectTypes, columnName)))
                        case Some(value) =>
                          decodersByName(value).parse(row, columnName)
                            .map(Some(_))
                            .asInstanceOf[Either[List[CsvDecoderError], Option[A]]]
                      }
                  }
              }
            }
          }
      }
      addAnnotations(decoder, enumAnnotations)
    }

    @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
    private def genericRecordConverter(
      id: TypeId,
      fieldSet: FieldSet,
      annotations: Chunk[Any]
    ): CsvDecoder[ListMap[String, _]] = {
      val parsed = extractAnnotations[ListMap[String, _]](annotations)
      convertUsingCache(id, parsed) {
        val fieldDecoders = fieldSet.toChunk.iterator
          .map { field =>
            val fieldDecoder = addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
            field.name.toString -> fieldDecoder
          }
          .toMap
        val baseDecoder = new CsvDecoder[ListMap[String, _]] {
          override val isOptional = false
          override val isPrimitive = false
          override def parseAsOption(
            row: Map[String, String],
            columnName: Option[String]
          ): Either[List[CsvDecoderError], Option[ListMap[String, _]]] = {
            // if parsing all fields gives Right(None) then the parsing result is Right(None)
            val errors = new ListBuffer[CsvDecoderError]
            val values = ListMap.newBuilder[String, Any]
            var someCount = 0

            for ((fieldLabel, fieldDecoder) <- fieldDecoders) {
              fieldDecoder.parseAsOption(row, Some(options.nestedFieldLabel(columnName, fieldLabel))) match {
                case Right(Some(value)) =>
                  someCount += 1
                  values.addOne((fieldLabel, value))
                case Right(None) =>
                  values.addOne((fieldLabel, None))
                case Left(errs) => errors.addAll(errs)
              }
            }

            if (errors.nonEmpty) Left(errors.toList)
            else if (someCount == 0) Right(None)
            else if (someCount < fieldDecoders.size) {
              Left(List(CsvDecoderError.notAllRequiredColumnsExist(errorColumn(options, columnName))))
            }
            else Right(Some(values.result()))
          }

          override def parse(
            row: Map[String, String],
            column: Option[String]
          ): Either[List[CsvDecoderError], ListMap[String, _]] = {
            val errors = new ListBuffer[CsvDecoderError]
            val values = ListMap.newBuilder[String, Any]

            for ((fieldLabel, fieldDecoder) <- fieldDecoders) {
              fieldDecoder.parse(row, Some(options.nestedFieldLabel(column, fieldLabel))) match {
                case Right(value) => values.addOne((fieldLabel, value))
                case Left(errs) => errors.addAll(errs)
              }
            }

            if (errors.isEmpty) Right(values.result())
            else Left(errors.toList)
          }
        }
        addAnnotations(baseDecoder, parsed)
      }
    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): CsvDecoder[A] = {
      val baseDecoder = standardType match {
        case StandardType.UnitType => unitDecoder
        case StandardType.StringType => stringDecoder
        case StandardType.BoolType => boolDecoder
        case StandardType.ByteType => byteDecoder
        case StandardType.ShortType => shortDecoder
        case StandardType.IntType => intDecoder
        case StandardType.LongType => longDecoder
        case StandardType.FloatType => floatDecoder
        case StandardType.DoubleType => doubleDecoder
        case StandardType.BinaryType => notSupported("BinaryType")
        case StandardType.CharType => charDecoder
        case StandardType.UUIDType => uuidDecoder
        case StandardType.BigDecimalType => bigDecimalDecoder
        case StandardType.BigIntegerType => bigIntDecoder
        case StandardType.DayOfWeekType => dayOfWeekDecoder
        case StandardType.MonthType => notSupported("MonthType")
        case StandardType.MonthDayType => notSupported("MonthDayType")
        case StandardType.PeriodType => notSupported("PeriodType")
        case StandardType.YearType => notSupported("YearType")
        case StandardType.YearMonthType => notSupported("YearMonthType")
        case StandardType.ZoneIdType => zoneIdDecoder
        case StandardType.ZoneOffsetType => zoneOffsetDecoder
        case StandardType.DurationType => durationDecoder
        case StandardType.InstantType => instantDecoder
        case StandardType.LocalDateType => localDateDecoder
        case StandardType.LocalTimeType => localTimeDecoder
        case StandardType.LocalDateTimeType => localDateTimeDecoder
        case StandardType.OffsetTimeType => offsetTimeDecoder
        case StandardType.OffsetDateTimeType => offsetDateTimeDecoder
        case StandardType.ZonedDateTimeType => zonedDateTimeDecoder
      }
      addAnnotations(baseDecoder.asInstanceOf[CsvDecoder[A]], extractAnnotations(annotations))
    }

    private def addAnnotations[A](
      baseDecoder: CsvDecoder[A],
      metadata: OpenApiParsedAnnotations[A]
    ): CsvDecoder[A] = {
      var decoder = baseDecoder
      decoder = metadata.default.fold(decoder) { case (default, _) =>
        decodeOption[A](decoder).map(maybeValue => maybeValue.getOrElse(default))
      }
      decoder = metadata.validator.fold(decoder) { validator: Validator[A] =>
        decoder.ensure(
          errors = a => validator(a).map(OpenApiValidation.errorMessage),
          options = options
        )
      }
      decoder
    }

    private def notSupported(value: String) = {
      throw new IllegalArgumentException(
        s"Cannot convert ZIO-Schema to CSV Decoder, because $value is currently not supported."
      )
    }
  }

}
