package dev.chopsticks.csv

import dev.chopsticks.openapi.{OpenApiParsedAnnotations, OpenApiSumTypeSerDeStrategy, OpenApiValidation}
import sttp.tapir.Validator
import zio.schema.{FieldSet, Schema, StandardType}
import zio.Chunk
import zio.schema.Schema.Primitive

import java.time.{Instant, LocalDate}
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

  private val stringDecoder: CsvDecoder[String] = createPrimitivePure(identity)

  private val instantDecoder: CsvDecoder[Instant] = {
    createFromThrowing(Instant.parse, s"Cannot parse timestamp; it must be in ISO-8601 format.")
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

  private class Converter(options: CsvCodecOptions) {
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

        case Schema.SetSchema(_, _) =>
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

        case Schema.GenericRecord(fieldSet, annotations) =>
          genericRecordConverter(fieldSet, annotations)

        case Schema.CaseClass1(f1, construct, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val baseDecoder = forProduct1(
            options = options,
            names = ArraySeq(f1.label),
            decoders = ArraySeq(decoder1)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass2(f1, f2, construct, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val baseDecoder = forProduct2(
            options = options,
            names = ArraySeq(f1.label, f2.label),
            decoders = ArraySeq(decoder1, decoder2)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass3(f1, f2, f3, construct, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val baseDecoder = forProduct3(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass4(f1, f2, f3, f4, construct, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val baseDecoder = forProduct4(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass5(f1, f2, f3, f4, f5, construct, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
          val baseDecoder = forProduct5(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass6(f1, f2, f3, f4, f5, f6, construct, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
          val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
          val baseDecoder = forProduct6(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, construct, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
          val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
          val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
          val baseDecoder = forProduct7(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, construct, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
          val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
          val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
          val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
          val baseDecoder = forProduct8(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, construct, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          val decoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
          val decoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
          val decoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
          val decoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
          val decoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
          val decoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
          val decoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
          val decoder8 = addAnnotations(convert(f8.schema), extractAnnotations(f8.annotations))
          val decoder9 = addAnnotations(convert(f9.schema), extractAnnotations(f9.annotations))
          val baseDecoder = forProduct9(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, construct, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct10(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, construct, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct11(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, construct, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct12(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct13(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct14(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct15(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct16(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct17(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct18(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label, f18.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct19(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label, f18.label, f19.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct20(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label, f18.label, f19.label, f20.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct21(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label, f18.label, f19.label, f20.label, f21.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, annotations) =>
          val parsed = extractAnnotations[A](annotations)
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
          val baseDecoder = forProduct22(
            options = options,
            names = ArraySeq(f1.label, f2.label, f3.label, f4.label, f5.label, f6.label, f7.label, f8.label, f9.label, f10.label, f11.label, f12.label, f13.label, f14.label, f15.label, f16.label, f17.label, f18.label, f19.label, f20.label, f21.label, f22.label),
            decoders = ArraySeq(decoder1, decoder2, decoder3, decoder4, decoder5, decoder6, decoder7, decoder8, decoder9, decoder10, decoder11, decoder12, decoder13, decoder14, decoder15, decoder16, decoder17, decoder18, decoder19, decoder20, decoder21, decoder22)
          )(construct)
          addAnnotations(baseDecoder, parsed)

        case Schema.Enum1(c1, annotations) =>
          convertEnum[A](annotations, c1)

        case Schema.Enum2(c1, c2, annotations) =>
          convertEnum[A](annotations, c1, c2)

        case Schema.Enum3(c1, c2, c3, annotations) =>
          convertEnum[A](annotations, c1, c2, c3)

        case Schema.Enum4(c1, c2, c3, c4, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4)

        case Schema.Enum5(c1, c2, c3, c4, c5, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5)

        case Schema.Enum6(c1, c2, c3, c4, c5, c6, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6)

        case Schema.Enum7(c1, c2, c3, c4, c5, c6, c7, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7)

        case Schema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8)

        case Schema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)

        case Schema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

        case Schema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)

        case Schema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)

        case Schema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)

        case Schema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)

        case Schema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)

        case Schema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)

        case Schema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)

        case Schema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)

        case Schema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)

        case Schema.Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)

        case Schema.Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)

        case Schema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
          convertEnum[A](annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)

        case _ =>
          ???
      }

    // scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

    private def convertEnum[A](
      annotations: Chunk[Any],
      cases: Schema.Case[_, A]*
    ): CsvDecoder[A] = {
      val enumAnnotations = extractAnnotations[A](annotations)
      val decodersByName = cases.iterator
        .map { c =>
          val cAnn = extractAnnotations(c.annotations)
          val decoder = addAnnotations(
            convert(c.codec),
            extractAnnotations(c.annotations)
          ).asInstanceOf[CsvDecoder[Any]]
          val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive a CsvDecoder. Received annotations: $cAnn"
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
      fieldSet: FieldSet,
      annotations: Chunk[Any]
    ): CsvDecoder[ListMap[String, _]] = {
      val parsed = extractAnnotations[ListMap[String, _]](annotations)
      val fieldDecoders = fieldSet.toChunk.iterator
        .map { field =>
          val fieldDecoder = addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
          field.label -> fieldDecoder
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

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): CsvDecoder[A] = {
      val baseDecoder = standardType match {
        case StandardType.UnitType => unitDecoder
        case StandardType.StringType => stringDecoder
        case StandardType.BoolType => boolDecoder
        case StandardType.ShortType => shortDecoder
        case StandardType.IntType => intDecoder
        case StandardType.LongType => longDecoder
        case StandardType.FloatType => notSupported("FloatType")
        case StandardType.DoubleType => notSupported("DoubleType")
        case StandardType.BinaryType => notSupported("BinaryType")
        case StandardType.CharType => notSupported("CharType")
        case StandardType.UUIDType => uuidDecoder
        case StandardType.BigDecimalType => bigDecimalDecoder
        case StandardType.BigIntegerType => bigIntDecoder
        case StandardType.DayOfWeekType => notSupported("DayOfWeekType")
        case StandardType.MonthType => notSupported("MonthType")
        case StandardType.MonthDayType => notSupported("MonthDayType")
        case StandardType.PeriodType => notSupported("PeriodType")
        case StandardType.YearType => notSupported("YearType")
        case StandardType.YearMonthType => notSupported("YearMonthType")
        case StandardType.ZoneIdType => notSupported("ZoneIdType")
        case StandardType.ZoneOffsetType => notSupported("ZoneOffsetType")
        case StandardType.DurationType => notSupported("DurationType")
        case StandardType.InstantType(_) => instantDecoder
        case StandardType.LocalDateType(_) => localDateDecoder
        case StandardType.LocalTimeType(_) => notSupported("LocalTimeType")
        case StandardType.LocalDateTimeType(_) => notSupported("LocalDateTimeType")
        case StandardType.OffsetTimeType(_) => notSupported("OffsetTimeType")
        case StandardType.OffsetDateTimeType(_) => notSupported("OffsetDateTimeType")
        case StandardType.ZonedDateTimeType(_) => notSupported("ZonedDateTimeType")
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
