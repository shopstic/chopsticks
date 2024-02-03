package dev.chopsticks.csv

import dev.chopsticks.openapi.{OpenApiParsedAnnotations, OpenApiSumTypeSerDeStrategy}
import org.apache.commons.text.StringEscapeUtils
import zio.schema.{Schema, StandardType}
import zio.{Chunk, ChunkBuilder}
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
import scala.annotation.nowarn
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.language.existentials

final case class CsvEncodingOptions(columnSeparator: String, rowSeparator: String, escapeValue: String => String)
object CsvEncodingOptions {
  val default = CsvEncodingOptions(
    columnSeparator = ",",
    rowSeparator = "\n",
    escapeValue = value => StringEscapeUtils.escapeCsv(value)
  )
}

final case class CsvEncodingResult(headers: Chunk[String], rows: Chunk[Chunk[String]]) {
  def toCsvString(encodingOptions: CsvEncodingOptions = CsvEncodingOptions.default): String = {
    Iterator
      .single {
        headers
          .iterator
          .map(encodingOptions.escapeValue)
          .mkString(encodingOptions.columnSeparator)
      }
      .concat {
        rows
          .iterator
          .map { row =>
            row
              .map(encodingOptions.escapeValue)
              .mkString(encodingOptions.columnSeparator)
          }
      }
      .mkString(encodingOptions.rowSeparator)
  }
}

trait CsvEncoder[A] { self =>
  def encodeSeq(values: Iterable[A]): CsvEncodingResult = {
    val encodedValues = Chunk.fromIterable(values).map(v => encode(v))
    val headers = encodedValues
      .foldLeft(mutable.SortedSet.empty[String]) { case (acc, next) =>
        acc ++ next.keys
      }
    val singleRowBuilder = ChunkBuilder.make[String](headers.size)
    val rows = encodedValues
      .foldLeft(ChunkBuilder.make[Chunk[String]](values.size)) { case (acc, next) =>
        singleRowBuilder.clear()
        val row = headers
          .foldLeft(singleRowBuilder) { case (rowBuilder, header) =>
            rowBuilder += next.getOrElse(header, "")
          }
          .result()
        acc += row
      }
      .result()

    CsvEncodingResult(Chunk.fromIterable(headers), Chunk.fromIterable(rows))
  }

  def encode(value: A): mutable.LinkedHashMap[String, String] =
    encode(value, columnName = None, mutable.LinkedHashMap.empty)
  def encode(
    value: A,
    columnName: Option[String],
    acc: mutable.LinkedHashMap[String, String]
  ): mutable.LinkedHashMap[String, String]

  final def contramap[B](f: B => A): CsvEncoder[B] = new CsvEncoder[B] {
    override def encode(
      value: B,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ): mutable.LinkedHashMap[String, String] = {
      self.encode(f(value), columnName, acc)
    }
  }

}

object CsvEncoder {
  import dev.chopsticks.openapi.OpenApiParsedAnnotations._

  final private case class CacheKey(entityName: String, annotationsHash: Int)

  final private[csv] class LazyEncoder[A]() extends CsvEncoder[A] {
    private var _encoder: CsvEncoder[A] = _
    private[CsvEncoder] def set(encoder: CsvEncoder[A]): Unit =
      this._encoder = encoder
    private def get: CsvEncoder[A] =
      if (_encoder == null) throw new RuntimeException("LazyEncoder has not yet been initialized")
      else _encoder

    override def encode(
      value: A,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ): mutable.LinkedHashMap[String, String] = {
      get.encode(value, columnName, acc)
    }
  }

  def derive[A](options: CsvCodecOptions = CsvCodecOptions.default)(implicit
    schema: Schema[A]
  ): CsvEncoder[A] = {
    new Converter(options, mutable.Map.empty).convert(schema)
  }

  private val unitEncoder: CsvEncoder[Unit] = new CsvEncoder[Unit] {
    override def encode(value: Unit, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), "")
      acc
    }
  }

  private val boolEncoder: CsvEncoder[Boolean] = new CsvEncoder[Boolean] {
    override def encode(value: Boolean, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val shortEncoder: CsvEncoder[Short] = new CsvEncoder[Short] {
    override def encode(value: Short, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val intEncoder: CsvEncoder[Int] = new CsvEncoder[Int] {
    override def encode(value: Int, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val longEncoder: CsvEncoder[Long] = new CsvEncoder[Long] {
    override def encode(value: Long, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val floatEncoder: CsvEncoder[Float] = new CsvEncoder[Float] {
    override def encode(value: Float, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val doubleEncoder: CsvEncoder[Double] = new CsvEncoder[Double] {
    override def encode(value: Double, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val stringEncoder: CsvEncoder[String] = new CsvEncoder[String] {
    override def encode(value: String, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value)
      acc
    }
  }

  private val charEncoder: CsvEncoder[Char] = stringEncoder.contramap[Char](_.toString)

  private val instantEncoder: CsvEncoder[Instant] = new CsvEncoder[Instant] {
    override def encode(value: Instant, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val bigDecimalEncoder: CsvEncoder[java.math.BigDecimal] = new CsvEncoder[java.math.BigDecimal] {
    override def encode(
      value: java.math.BigDecimal,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val bigIntEncoder: CsvEncoder[java.math.BigInteger] = new CsvEncoder[java.math.BigInteger] {
    override def encode(
      value: java.math.BigInteger,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val localDateEncoder: CsvEncoder[LocalDate] = new CsvEncoder[LocalDate] {
    override def encode(value: LocalDate, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val uuidEncoder: CsvEncoder[UUID] = new CsvEncoder[UUID] {
    override def encode(value: UUID, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val dayOfWeekEncoder: CsvEncoder[DayOfWeek] = new CsvEncoder[DayOfWeek] {
    override def encode(value: DayOfWeek, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val durationEncoder: CsvEncoder[Duration] = new CsvEncoder[Duration] {
    override def encode(value: Duration, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val localTimeEncoder: CsvEncoder[LocalTime] = new CsvEncoder[LocalTime] {
    override def encode(value: LocalTime, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val localDateTimeEncoder: CsvEncoder[LocalDateTime] = new CsvEncoder[LocalDateTime] {
    override def encode(
      value: LocalDateTime,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val offsetTimeEncoder: CsvEncoder[OffsetTime] = new CsvEncoder[OffsetTime] {
    override def encode(
      value: OffsetTime,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val offsetDateTimeEncoder: CsvEncoder[OffsetDateTime] = new CsvEncoder[OffsetDateTime] {
    override def encode(
      value: OffsetDateTime,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val zonedDateTimeEncoder: CsvEncoder[ZonedDateTime] = new CsvEncoder[ZonedDateTime] {
    override def encode(
      value: ZonedDateTime,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val zoneIdEncoder: CsvEncoder[ZoneId] = new CsvEncoder[ZoneId] {
    override def encode(
      value: ZoneId,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private val zoneOffsetEncoder: CsvEncoder[ZoneOffset] = new CsvEncoder[ZoneOffset] {
    override def encode(
      value: ZoneOffset,
      columnName: Option[String],
      acc: mutable.LinkedHashMap[String, String]
    ) = {
      val _ = acc.put(columnName.getOrElse(""), value.toString)
      acc
    }
  }

  private def encodeOption[A](e: CsvEncoder[A]): CsvEncoder[Option[A]] =
    new CsvEncoder[Option[A]] {
      override def encode(
        value: Option[A],
        columnName: Option[String],
        acc: mutable.LinkedHashMap[String, String]
      ): mutable.LinkedHashMap[String, String] = {
        value match {
          case Some(v) => e.encode(v, columnName, acc)
          case None => acc
        }
      }
    }

  private def encodeChunk[A](e: CsvEncoder[A], options: CsvCodecOptions): CsvEncoder[Chunk[A]] =
    new CsvEncoder[Chunk[A]] {
      override def encode(
        value: Chunk[A],
        columnName: Option[String],
        acc: mutable.LinkedHashMap[String, String]
      ): mutable.LinkedHashMap[String, String] = {
        value.zipWithIndex.foldLeft(acc) { case (res, (v, i)) =>
          e.encode(v, Some(options.nestedArrayFieldLabel(columnName, i)), res)
        }
      }
    }

  private class Converter(options: CsvCodecOptions, cache: scala.collection.mutable.Map[CacheKey, LazyEncoder[_]]) {
    private def convertUsingCache[A](annotations: OpenApiParsedAnnotations[A])(convert: => CsvEncoder[A])
      : CsvEncoder[A] = {
      annotations.entityName match {
        case Some(name) =>
          val cacheKey = CacheKey(name, annotations.hashCode())
          cache.get(cacheKey) match {
            case Some(value) => value.asInstanceOf[CsvEncoder[A]]
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

    // scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
    def convert[A](schema: Schema[A] /*, modifiers: Any => Any*/ ): CsvEncoder[A] =
      schema match {
        case Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, _, toChunk, annotations, _) =>
          addAnnotations(
            encodeChunk(convert(schemaA), options).contramap(toChunk),
            extractAnnotations(annotations)
          )

        case Schema.SetSchema(_, _) =>
          ???

        case Schema.Transform(schema, _, g, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseEncoder = convert(schema).contramap[A] { x =>
            g(x) match {
              case Right(v) => v
              case Left(error) => throw new RuntimeException(s"Couldn't transform schema: $error")
            }
          }
          addAnnotations(baseEncoder, typedAnnotations)

        case Schema.Optional(schema, annotations) =>
          addAnnotations[A](
            baseEncoder = encodeOption(convert(schema)).asInstanceOf[CsvEncoder[A]],
            metadata = extractAnnotations(annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema)

        case Schema.GenericRecord(fieldSet, annotations) =>
          val recordAnnotations: OpenApiParsedAnnotations[A] = extractAnnotations[A](annotations)
          convertUsingCache(recordAnnotations) {
            val fieldEncoders = fieldSet.toChunk
              .map { field =>
                addAnnotations(convert(field.schema), extractAnnotations(field.annotations))
              }
            val baseEncoder = new CsvEncoder[ListMap[String, _]] {
              override def encode(value: ListMap[String, _], columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                value.iterator
                  .zip(fieldEncoders.iterator)
                  .foldLeft(acc) { case (res, ((k, v), encoder)) =>
                    encoder.asInstanceOf[CsvEncoder[Any]].encode(v.asInstanceOf[Any], Some(k), res)
                  }
              }
            }
            addAnnotations(baseEncoder, recordAnnotations)
          }

        case Schema.CaseClass1(f1, _, ext1, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass2(f1, f2, _, ext1, ext2, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass3(f1, f2, f3, _, ext1, ext2, ext3, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass4(f1, f2, f3, f4, _, ext1, ext2, ext3, ext4, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
            val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass5(f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
            val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
            val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass6(f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
            val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
            val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
            val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          convertUsingCache(parsed) {
            val encoder1 = addAnnotations(convert(f1.schema), extractAnnotations(f1.annotations))
            val encoder2 = addAnnotations(convert(f2.schema), extractAnnotations(f2.annotations))
            val encoder3 = addAnnotations(convert(f3.schema), extractAnnotations(f3.annotations))
            val encoder4 = addAnnotations(convert(f4.schema), extractAnnotations(f4.annotations))
            val encoder5 = addAnnotations(convert(f5.schema), extractAnnotations(f5.annotations))
            val encoder6 = addAnnotations(convert(f6.schema), extractAnnotations(f6.annotations))
            val encoder7 = addAnnotations(convert(f7.schema), extractAnnotations(f7.annotations))
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res = encoder18.encode(ext18(value), Some(options.nestedFieldLabel(columnName, f18.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res = encoder18.encode(ext18(value), Some(options.nestedFieldLabel(columnName, f18.label)), res)
                res = encoder19.encode(ext19(value), Some(options.nestedFieldLabel(columnName, f19.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res = encoder18.encode(ext18(value), Some(options.nestedFieldLabel(columnName, f18.label)), res)
                res = encoder19.encode(ext19(value), Some(options.nestedFieldLabel(columnName, f19.label)), res)
                res = encoder20.encode(ext20(value), Some(options.nestedFieldLabel(columnName, f20.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res = encoder18.encode(ext18(value), Some(options.nestedFieldLabel(columnName, f18.label)), res)
                res = encoder19.encode(ext19(value), Some(options.nestedFieldLabel(columnName, f19.label)), res)
                res = encoder20.encode(ext20(value), Some(options.nestedFieldLabel(columnName, f20.label)), res)
                res = encoder21.encode(ext21(value), Some(options.nestedFieldLabel(columnName, f21.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

        case Schema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22, annotations) =>
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
            val baseEncoder = new CsvEncoder[A] {
              override def encode(value: A, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]): mutable.LinkedHashMap[String, String] = {
                var res = acc
                res = encoder1.encode(ext1(value), Some(options.nestedFieldLabel(columnName, f1.label)), res)
                res = encoder2.encode(ext2(value), Some(options.nestedFieldLabel(columnName, f2.label)), res)
                res = encoder3.encode(ext3(value), Some(options.nestedFieldLabel(columnName, f3.label)), res)
                res = encoder4.encode(ext4(value), Some(options.nestedFieldLabel(columnName, f4.label)), res)
                res = encoder5.encode(ext5(value), Some(options.nestedFieldLabel(columnName, f5.label)), res)
                res = encoder6.encode(ext6(value), Some(options.nestedFieldLabel(columnName, f6.label)), res)
                res = encoder7.encode(ext7(value), Some(options.nestedFieldLabel(columnName, f7.label)), res)
                res = encoder8.encode(ext8(value), Some(options.nestedFieldLabel(columnName, f8.label)), res)
                res = encoder9.encode(ext9(value), Some(options.nestedFieldLabel(columnName, f9.label)), res)
                res = encoder10.encode(ext10(value), Some(options.nestedFieldLabel(columnName, f10.label)), res)
                res = encoder11.encode(ext11(value), Some(options.nestedFieldLabel(columnName, f11.label)), res)
                res = encoder12.encode(ext12(value), Some(options.nestedFieldLabel(columnName, f12.label)), res)
                res = encoder13.encode(ext13(value), Some(options.nestedFieldLabel(columnName, f13.label)), res)
                res = encoder14.encode(ext14(value), Some(options.nestedFieldLabel(columnName, f14.label)), res)
                res = encoder15.encode(ext15(value), Some(options.nestedFieldLabel(columnName, f15.label)), res)
                res = encoder16.encode(ext16(value), Some(options.nestedFieldLabel(columnName, f16.label)), res)
                res = encoder17.encode(ext17(value), Some(options.nestedFieldLabel(columnName, f17.label)), res)
                res = encoder18.encode(ext18(value), Some(options.nestedFieldLabel(columnName, f18.label)), res)
                res = encoder19.encode(ext19(value), Some(options.nestedFieldLabel(columnName, f19.label)), res)
                res = encoder20.encode(ext20(value), Some(options.nestedFieldLabel(columnName, f20.label)), res)
                res = encoder21.encode(ext21(value), Some(options.nestedFieldLabel(columnName, f21.label)), res)
                res = encoder22.encode(ext22(value), Some(options.nestedFieldLabel(columnName, f22.label)), res)
                res
              }
            }
            addAnnotations(baseEncoder, parsed)
          }

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
    ): CsvEncoder[A] = {
      val enumAnnotations = extractAnnotations[A](annotations)
      val encodersByName = cases.iterator
        .map { c =>
          val cAnn = extractAnnotations(c.annotations)
          val encoder = addAnnotations(
            convert(c.codec),
            extractAnnotations(c.annotations)
          ).asInstanceOf[CsvEncoder[Any]]
          val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive a CsvEncoder. Received annotations: $cAnn"
          ))
          entityName -> (encoder, c)
        }
        .toMap
      val discriminator = enumAnnotations.sumTypeSerDeStrategy

      val decoder = discriminator
        .getOrElse(throw new RuntimeException(
          s"Discriminator must be defined to derive an CsvEncoder. Received annotations: $enumAnnotations"
        )) match {
        case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
          val diff = discriminator.mapping.values.toSet.diff(encodersByName.keySet)
          if (diff.nonEmpty) {
            throw new RuntimeException(
              s"Cannot derive CsvEncoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and decoders don't match. Diff=$diff."
            )
          }
          new CsvEncoder[A] {
            override def encode(
              value: A,
              columnName: Option[String],
              acc: mutable.LinkedHashMap[String, String]
            ): mutable.LinkedHashMap[String, String] = {
              var res = acc
              val discValue = discriminator.discriminatorValue(value)
              val (enc, c) = encodersByName(discriminator.mapping(discValue))
              val discriminatorColumnName =
                Some(options.nestedFieldLabel(columnName, discriminator.discriminatorFieldName))
              res = stringEncoder.encode(discriminator.discriminatorFieldName, discriminatorColumnName, res)
              enc.encode(c.unsafeDeconstruct(value).asInstanceOf[Any], columnName, res)
            }
          }
      }
      addAnnotations(decoder, enumAnnotations)
    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): CsvEncoder[A] = {
      val baseEncoder = standardType match {
        case StandardType.UnitType => unitEncoder
        case StandardType.StringType => stringEncoder
        case StandardType.BoolType => boolEncoder
        case StandardType.ShortType => shortEncoder
        case StandardType.IntType => intEncoder
        case StandardType.LongType => longEncoder
        case StandardType.FloatType => floatEncoder
        case StandardType.DoubleType => doubleEncoder
        case StandardType.BinaryType => notSupported("BinaryType")
        case StandardType.CharType => charEncoder
        case StandardType.UUIDType => uuidEncoder
        case StandardType.BigDecimalType => bigDecimalEncoder
        case StandardType.BigIntegerType => bigIntEncoder
        case StandardType.DayOfWeekType => dayOfWeekEncoder
        case StandardType.MonthType => notSupported("MonthType")
        case StandardType.MonthDayType => notSupported("MonthDayType")
        case StandardType.PeriodType => notSupported("PeriodType")
        case StandardType.YearType => notSupported("YearType")
        case StandardType.YearMonthType => notSupported("YearMonthType")
        case StandardType.ZoneIdType => zoneIdEncoder
        case StandardType.ZoneOffsetType => zoneOffsetEncoder
        case StandardType.DurationType => durationEncoder
        case StandardType.InstantType(_) => instantEncoder
        case StandardType.LocalDateType(_) => localDateEncoder
        case StandardType.LocalTimeType(_) => localTimeEncoder
        case StandardType.LocalDateTimeType(_) => localDateTimeEncoder
        case StandardType.OffsetTimeType(_) => offsetTimeEncoder
        case StandardType.OffsetDateTimeType(_) => offsetDateTimeEncoder
        case StandardType.ZonedDateTimeType(_) => zonedDateTimeEncoder
      }
      addAnnotations(baseEncoder.asInstanceOf[CsvEncoder[A]], extractAnnotations(annotations))
    }

    @nowarn("cat=unused-params")
    private def addAnnotations[A](
      baseEncoder: CsvEncoder[A],
      metadata: OpenApiParsedAnnotations[A]
    ): CsvEncoder[A] = {
      baseEncoder
    }

    private def notSupported(value: String) = {
      throw new IllegalArgumentException(
        s"Cannot convert ZIO-Schema to CSV Encoder, because $value is currently not supported."
      )
    }
  }

}
