package dev.chopsticks.csv

import dev.chopsticks.openapi.{OpenApiParsedAnnotations, OpenApiSumTypeSerDeStrategy}
import dev.chopsticks.openapi.common.{ConverterCache, OpenApiConverterUtils}
import org.apache.commons.text.StringEscapeUtils
import zio.schema.{Schema, StandardType, TypeId}
import zio.{Chunk, ChunkBuilder}
import zio.schema.Schema.{Field, Primitive}

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

trait CsvEncoder[A] {
  self =>
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

  final private[csv] class LazyEncoder[A] extends CsvEncoder[A] with ConverterCache.Lazy[CsvEncoder[A]] {
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
    new Converter(options).convert(schema)
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

  private val byteEncoder: CsvEncoder[Byte] = new CsvEncoder[Byte] {
    override def encode(value: Byte, columnName: Option[String], acc: mutable.LinkedHashMap[String, String]) = {
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

  private class Converter(
    options: CsvCodecOptions,
    cache: ConverterCache[CsvEncoder] = new ConverterCache[CsvEncoder]()
  ) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: OpenApiParsedAnnotations[A]
    )(convert: => CsvEncoder[A]): CsvEncoder[A] = {
      cache.convertUsingCache(typeId, annotations)(convert)(() => new LazyEncoder[A]())
    }

    // scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
    def convert[A](schema: Schema[A] /*, modifiers: Any => Any*/ ): CsvEncoder[A] =
      schema match {
        case Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, _, toChunk, annotations, _) =>
          addAnnotations(
            None,
            encodeChunk(convert(schemaA), options).contramap(toChunk),
            extractAnnotations(annotations)
          )

        case Schema.Set(_, _) =>
          ???

        case Schema.Transform(schema, _, g, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseEncoder = convert(schema).contramap[A] { x =>
            g(x) match {
              case Right(v) => v
              case Left(error) => throw new RuntimeException(s"Couldn't transform schema: $error")
            }
          }
          addAnnotations(None, baseEncoder, typedAnnotations)

        case Schema.Optional(schema, annotations) =>
          addAnnotations[A](
            None,
            baseEncoder = encodeOption(convert(schema)).asInstanceOf[CsvEncoder[A]],
            metadata = extractAnnotations(annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema)

        case s: Schema.Record[A] =>
          convertRecord[A](s.id, s.annotations, s.fields)

        case s: Schema.Enum[A] =>
          convertEnum[A](s.id, s.annotations, s.cases)

        case _ =>
          ???
      }

    // scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

    private def convertRecord[A](
      id: TypeId,
      annotations: Chunk[Any],
      fields: Chunk[Field[A, _]]
    ): CsvEncoder[A] = {
      val recordAnnotations: OpenApiParsedAnnotations[A] = extractAnnotations[A](annotations)
      convertUsingCache(id, recordAnnotations) {
        val fieldEncoders = fields
          .map { field =>
            addAnnotations(None, convert(field.schema), extractAnnotations(field.annotations))
          }
        val baseEncoder = new CsvEncoder[A] {
          override def encode(
            value: A,
            columnName: Option[String],
            acc: mutable.LinkedHashMap[String, String]
          ): mutable.LinkedHashMap[String, String] = {
            var res = acc
            var i = 0
            while (i < fields.length) {
              val field = fields(i)
              val encoder = fieldEncoders(i)
              res = encoder
                .asInstanceOf[CsvEncoder[Any]]
                .encode(field.get(value), Some(options.nestedFieldLabel(columnName, field.name.toString)), res)
              i += 1
            }
            res
          }
        }
        addAnnotations(Some(id), baseEncoder, recordAnnotations)
      }
    }

    private def convertEnum[A](
      id: TypeId,
      annotations: Chunk[Any],
      cases: Chunk[Schema.Case[A, _]]
    ): CsvEncoder[A] = {
      val enumAnnotations = extractAnnotations[A](annotations)
      val encodersByName = cases.iterator
        .map { c =>
          val cAnn = extractAnnotations(c.annotations)
          val encoder = addAnnotations(
            None,
            convert(c.schema),
            extractAnnotations(c.annotations)
          ).asInstanceOf[CsvEncoder[Any]]
          val entityName = OpenApiConverterUtils.getCaseEntityName(c, cAnn).getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined or be a case class to derive a CsvEncoder. Received annotations: $cAnn"
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
              enc.encode(c.deconstruct(value).asInstanceOf[Any], columnName, res)
            }
          }
      }
      addAnnotations(Some(id), decoder, enumAnnotations)
    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): CsvEncoder[A] = {
      val baseEncoder = standardType match {
        case StandardType.UnitType => unitEncoder
        case StandardType.StringType => stringEncoder
        case StandardType.BoolType => boolEncoder
        case StandardType.ByteType => byteEncoder
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
        case StandardType.InstantType => instantEncoder
        case StandardType.LocalDateType => localDateEncoder
        case StandardType.LocalTimeType => localTimeEncoder
        case StandardType.LocalDateTimeType => localDateTimeEncoder
        case StandardType.OffsetTimeType => offsetTimeEncoder
        case StandardType.OffsetDateTimeType => offsetDateTimeEncoder
        case StandardType.ZonedDateTimeType => zonedDateTimeEncoder
      }
      addAnnotations(None, baseEncoder.asInstanceOf[CsvEncoder[A]], extractAnnotations(annotations))
    }

    @nowarn("cat=unused-params")
    private def addAnnotations[A](
      typeId: Option[TypeId],
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
