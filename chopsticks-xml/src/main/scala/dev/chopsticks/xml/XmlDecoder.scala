package dev.chopsticks.xml

import dev.chopsticks.openapi.common.ConverterCache
import dev.chopsticks.openapi.OpenApiValidation
import dev.chopsticks.xml.XmlAnnotations.extractAnnotations
import sttp.tapir.Validator
import zio.schema.{Schema, StandardType, TypeId}
import zio.schema.Schema.Primitive
import zio.Chunk

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
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.xml.NodeSeq

final case class XmlDecoderError(message: String, private[chopsticks] val reversedPath: List[String]) {
//  def format: String = {
//    val trimmed = message.trim
//    val withDot = if (trimmed.endsWith(".")) trimmed else trimmed + "."
//    withDot + (if (columnName.isDefined) s" Column(s): ${columnName.get}" else "")
//  }
  lazy val path: List[String] = reversedPath.reverse
}
object XmlDecoderError {
//  def columnNotExists(columnName: String): XmlDecoderError = {
//    XmlDecoderError(s"Required column does not exist.", columnName = Some(columnName))
//  }
//  def notAllRequiredColumnsExist(columnName: Option[String]) = {
//    XmlDecoderError("Not all required columns contain defined values.", columnName)
//  }
//  def notAllRequiredColumnsExist(columnNames: List[String]) = {
//    XmlDecoderError("Not all required columns contain defined values.", Some(columnNames.mkString(", ")))
//  }
//  def unrecognizedDiscriminatorType(received: String, formattedKnownTypes: String, columnName: Option[String]) = {
//    XmlDecoderError(
//      s"Unrecognized type: ${received}. Valid object types are: $formattedKnownTypes.",
//      columnName
//    )
//  }
}

trait XmlDecoder[A] { self =>
  def isOptional: Boolean
  def parse(node: NodeSeq): Either[List[XmlDecoderError], A] = parse(node, Nil)
  def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A]
  final def ensure(pred: A => Boolean, message: => String) =
    new XmlDecoder[A] {
      override val isOptional = self.isOptional
//      override val isPrimitive = self.isPrimitive
//      override def parseAsOption(row: Map[String, String], columnName: Option[String]) = {
//        self.parseAsOption(row, columnName) match {
//          case r @ Right(Some(value)) =>
//            if (pred(value)) r else Left(List(XmlDecoderError(message, errorColumn(options, columnName))))
//          case r @ Right(None) => r
//          case l @ Left(_) => l
//        }
//      }
      override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A] =
        self.parse(node, path) match {
          case r @ Right(value) =>
            if (pred(value)) r
            else Left(List(XmlDecoderError(message, path)))
          case l @ Left(_) => l
        }
    }

  final def ensure(errors: A => List[String]) = new XmlDecoder[A] {
    override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A] = {
      self.parse(node, path) match {
        case r @ Right(value) =>
          errors(value) match {
            case Nil => r
            case errs => Left(errs.map(e => XmlDecoderError(e, path)))
          }
        case l @ Left(_) => l
      }
    }
    override val isOptional = self.isOptional
//    override val isPrimitive = self.isPrimitive
//    override def parseAsOption(
//                                row: Map[String, String],
//                                columnName: Option[String]
//                              ): Either[List[XmlDecoderError], Option[A]] = {
//      self.parseAsOption(row, columnName) match {
//        case r @ Right(Some(value)) =>
//          errors(value) match {
//            case Nil => r
//            case errs =>
//              Left(errs.map(e => XmlDecoderError(e, errorColumn(options, columnName))))
//          }
//        case r @ Right(None) => r
//        case l @ Left(_) => l
//      }
  }

  final def map[B](f: A => B) = new XmlDecoder[B] {
    override def isOptional = self.isOptional
    override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], B] =
      self.parse(node, path).map(f)
  }

  final def emap[B](f: A => Either[String, B]) = new XmlDecoder[B] {
    override def isOptional = self.isOptional
    override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], B] =
      self.parse(node, path) match {
        case Left(errors) => Left(errors)
        case Right(value) =>
          f(value) match {
            case Left(error) => Left(List(XmlDecoderError(error, path)))
            case Right(b) => Right(b)
          }
      }
  }

}

object XmlDecoder {
  def derive[A]()(implicit schema: Schema[A]): XmlDecoder[A] = {
    new Converter().convert(schema, None)
  }

  private def createPrimitive[A](f: (String, List[String]) => Either[List[XmlDecoderError], A]): XmlDecoder[A] =
    new XmlDecoder[A] {
      override def isOptional = false
      override def parse(nodes: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A] = {
        if (nodes.length != 1) {
          Left(List(XmlDecoderError(s"Expected text value, but many nodes instead.", path)))
        }
        else {
          val node = nodes.head
          node match {
            case scala.xml.Text(data) => f(data, path)
            case other =>
              Left(List(XmlDecoderError(
                s"Expected text node, but got [${other.getClass.getSimpleName}]: $other.",
                path
              )))
          }
        }
      }
    }

  private def createPrimitiveFromThrowing[A](f: String => A, errorMessage: => String) = createPrimitive {
    case (value, maybeColumn) =>
      try Right(f(value))
      catch { case NonFatal(_) => Left(List(XmlDecoderError(errorMessage, maybeColumn))) }
  }

  private val unitDecoder: XmlDecoder[Unit] = createPrimitive((_, _) => Right(()))
  private val boolDecoder: XmlDecoder[Boolean] = createPrimitive { (x, path) =>
    x.toBooleanOption match {
      case Some(value) => Right(value)
      case None => Left(List(XmlDecoderError(s"Cannot parse boolean (it must be either 'true' or 'false').", path)))
    }
  }

  private val byteDecoder: XmlDecoder[Byte] = createPrimitive { case (value, col) =>
    value.toByteOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse byte.", col)))
    }
  }

  private val shortDecoder: XmlDecoder[Short] = createPrimitive { case (value, col) =>
    value.toShortOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val intDecoder: XmlDecoder[Int] = createPrimitive { case (value, col) =>
    value.toIntOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val longDecoder: XmlDecoder[Long] = createPrimitive { case (value, col) =>
    value.toLongOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val floatDecoder: XmlDecoder[Float] = createPrimitive { case (value, col) =>
    value.toFloatOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val doubleDecoder: XmlDecoder[Double] = createPrimitive { case (value, col) =>
    value.toDoubleOption match {
      case Some(n) => Right(n)
      case None => Left(List(XmlDecoderError(s"Cannot parse number.", col)))
    }
  }

  private val charDecoder: XmlDecoder[Char] = createPrimitive { case (value, col) =>
    if (value.length == 1) Right(value.charAt(0))
    else Left(List(XmlDecoderError(s"Cannot parse char. Got string with length ${value.length} instead.", col)))
  }

  private val stringDecoder: XmlDecoder[String] = createPrimitive((value, _) => Right(value))

  private val instantDecoder: XmlDecoder[Instant] = {
    createPrimitiveFromThrowing(Instant.parse, s"Cannot parse timestamp; it must be in ISO-8601 format.")
  }

  private val dayOfWeekDecoder: XmlDecoder[DayOfWeek] = {
    createPrimitiveFromThrowing(
      DayOfWeek.valueOf, {
        val expected = DayOfWeek.values().iterator.map(_.toString).mkString(", ")
        s"Cannot parse day of week. Expected one of: $expected."
      }
    )
  }

  private val durationDecoder: XmlDecoder[Duration] = {
    createPrimitiveFromThrowing(
      Duration.parse,
      s"Cannot parse duration; it must be in ISO-8601 format."
    )
  }

  private val localTimeDecoder: XmlDecoder[LocalTime] = {
    createPrimitiveFromThrowing(
      LocalTime.parse,
      s"""Cannot parse local time; expected text such as "10:15:00"."""
    )
  }

  private val localDateTimeDecoder: XmlDecoder[LocalDateTime] = {
    createPrimitiveFromThrowing(
      LocalDateTime.parse,
      s"""Cannot parse local date time; expected text such as "2020-12-03T10:15:30"."""
    )
  }

  private val offsetTimeDecoder: XmlDecoder[OffsetTime] = {
    createPrimitiveFromThrowing(
      OffsetTime.parse,
      s"""Cannot parse offset time; expected text such as "10:15:30+01:00"."""
    )
  }

  private val offsetDateTimeDecoder: XmlDecoder[OffsetDateTime] = {
    createPrimitiveFromThrowing(
      OffsetDateTime.parse,
      s"""Cannot parse offset date time; expected text such as "2020-12-03T10:15:30+01:00"."""
    )
  }

  private val zonedDateTimeDecoder: XmlDecoder[ZonedDateTime] = {
    createPrimitiveFromThrowing(
      ZonedDateTime.parse,
      s"""Cannot parse zoned date time; expected text such as "2020-12-03T10:15:30+01:00[Europe/Paris]"."""
    )
  }

  private val bigDecimalDecoder: XmlDecoder[java.math.BigDecimal] = {
    createPrimitiveFromThrowing(BigDecimal.apply(_).underlying(), s"Cannot parse BigDecimal number.")
  }

  private val bigIntDecoder: XmlDecoder[java.math.BigInteger] = {
    createPrimitiveFromThrowing(BigInt.apply(_).underlying(), s"Cannot parse BigInteger number.")
  }

  private val localDateDecoder: XmlDecoder[LocalDate] = {
    createPrimitiveFromThrowing(
      LocalDate.parse,
      s"Cannot parse date; it must be in ISO-8601 format (i.e. 'yyyy-MM-dd')."
    )
  }

  private val uuidDecoder: XmlDecoder[UUID] = {
    createPrimitiveFromThrowing(UUID.fromString, s"Cannot parse UUID.")
  }

  private val zoneIdDecoder: XmlDecoder[ZoneId] = {
    createPrimitiveFromThrowing(str => ZoneId.of(str), s"Cannot parse ZoneId.")
  }

  private val zoneOffsetDecoder: XmlDecoder[ZoneOffset] = {
    createPrimitiveFromThrowing(str => ZoneOffset.of(str), s"Cannot parse ZoneOffset.")
  }

  private def decodeOption[A](d: XmlDecoder[A]): XmlDecoder[Option[A]] =
    new XmlDecoder[Option[A]] {
      final override def isOptional: Boolean = true
      override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], Option[A]] = {
        if (node.isEmpty) Right(None)
        else d.parse(node, path).map(Some(_))
      }
    }

  private def decodeChunk[A](underlying: XmlDecoder[A], nodeName: String): XmlDecoder[Chunk[A]] =
    new XmlDecoder[Chunk[A]] {
      final override def isOptional: Boolean = false
      override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], Chunk[A]] = {
        val children = node.iterator.filter(_.label == nodeName).map(_.child)
        val errorBuilder = ListBuffer[XmlDecoderError]()
        val chunkBuilder = Chunk.newBuilder[A]

        var i = 0
        for (child <- children) {
          // XPath starts with 1
          underlying.parse(child, s"[${i + 1}]" :: path) match {
            case Right(value) =>
              val _ = chunkBuilder += value
            case Left(errors) =>
              val _ = errorBuilder ++= errors
          }
          i += 1
        }

        if (errorBuilder.isEmpty) Right(chunkBuilder.result())
        else Left(errorBuilder.toList)
      }
    }

  final private[xml] class LazyDecoder[A] extends XmlDecoder[A] with ConverterCache.Lazy[XmlDecoder[A]] {
    override def isOptional: Boolean = get.isOptional
//    override def isPrimitive: Boolean = get.isOptional

    override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A] = {
      get.parse(node, path)
    }
  }

  private class Converter(cache: ConverterCache[XmlDecoder] = new ConverterCache[XmlDecoder]()) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: XmlAnnotations[A]
    )(convert: => XmlDecoder[A]): XmlDecoder[A] = {
      cache.convertUsingCache(typeId, annotations.openApiAnnotations)(convert)(() => new LazyDecoder[A]())
    }

    def convert[A](schema: Schema[A], xmlSeqNodeName: Option[String]): XmlDecoder[A] = {
      schema match {
        case Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, fromChunk, _, annotations, _) =>
          val parsed = extractAnnotations[A](annotations)
          val nodeName = parsed.xmlSeqNodeName
            .orElse(xmlSeqNodeName)
            .getOrElse {
              throw new RuntimeException("Sequence must have xmlSeqNodeName annotation")
            }
          addAnnotations(
            decodeChunk(convert(schemaA, Some(nodeName)), nodeName).map(fromChunk),
            parsed
          )

        case Schema.Set(_, _) =>
          ???

        case Schema.Transform(schema, f, _, annotations, _) =>
          val parsed = extractAnnotations[A](annotations)
          val baseDecoder = convert(schema, parsed.xmlSeqNodeName).emap(f)
          addAnnotations(baseDecoder, parsed)

        case Schema.Optional(schema, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          addAnnotations[A](
            baseDecoder = decodeOption(convert(schema, parsed.xmlSeqNodeName)),
            metadata = parsed
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema, xmlSeqNodeName)

        case record: Schema.Record[A] =>
          convertRecord(record)

        case _ =>
          ???
      }
    }

    private def convertRecord[A](record: Schema.Record[A]): XmlDecoder[A] = {
      val recordAnnotations: XmlAnnotations[A] = extractAnnotations[A](record.annotations)
      convertUsingCache(record.id, recordAnnotations) {
        val fieldDecoders = record.fields
          .map { field =>
            val fieldAnnotations = extractAnnotations[Any](field.annotations)
            addAnnotations[Any](
              convert[Any](field.schema.asInstanceOf[Schema[Any]], fieldAnnotations.xmlSeqNodeName),
              fieldAnnotations
            )
          }
        val fieldNames = record.fields.map { field => extractAnnotations[Any](field.annotations).xmlFieldName }
        val baseDecoder = new XmlDecoder[A] {
          final override def isOptional: Boolean = false
          override def parse(node: NodeSeq, path: List[String]): Either[List[XmlDecoderError], A] = {
            val errorBuilder = ListBuffer.empty[XmlDecoderError]
            val argsBuilder = Chunk.newBuilder[Any]
            var i = 0
            while (i < record.fields.length) {
              val field = record.fields(i)
              val fieldName = fieldNames(i).getOrElse(field.name)
              val fieldDecoder = fieldDecoders(i)
              val childNodeIndex = node.indexWhere(_.label == fieldName)
              if (childNodeIndex == -1 && fieldDecoder.isOptional) {
                val _ = argsBuilder += None
              }
              else if (childNodeIndex == -1) {
                val _ = errorBuilder += XmlDecoderError(s"Expected field '$fieldName', but it was not found.", path)
              }
              else {
                val fieldNodes = node(childNodeIndex).child
                val result = fieldDecoder.parse(fieldNodes, fieldName :: path)
                result match {
                  case Right(value) =>
                    val _ = argsBuilder += value
                  case Left(errors) =>
                    val _ = errorBuilder ++= errors
                }
              }
              i += 1
            }

            if (errorBuilder.nonEmpty) Left(errorBuilder.toList)
            else {
              val args = argsBuilder.result()
              if (args.length != record.fields.length) {
                Left(List(XmlDecoderError(s"Expected ${record.fields.length} fields, but got ${args.length}.", path)))
              }
              else {
                val result = zio.Unsafe.unsafe { implicit unsafe =>
                  record.construct(args) match {
                    case Right(value) => value
                    case Left(error) =>
                      throw new RuntimeException(s"Error constructing record [${record.id.name}]: $error")
                  }
                }
                Right(result)
              }
            }
          }
        }
        addAnnotations(baseDecoder, recordAnnotations)
      }
    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): XmlDecoder[A] = {
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
      addAnnotations(baseDecoder.asInstanceOf[XmlDecoder[A]], extractAnnotations(annotations))
    }

    private def addAnnotations[A](
      baseDecoder: XmlDecoder[A],
      metadata: XmlAnnotations[A]
    ): XmlDecoder[A] = {
      var decoder = baseDecoder
      decoder = metadata.openApiAnnotations.default.fold(decoder) { case (default, _) =>
        decodeOption[A](decoder).map(maybeValue => maybeValue.getOrElse(default))
      }
      decoder = metadata.openApiAnnotations.validator.fold(decoder) { validator: Validator[A] =>
        decoder.ensure { value =>
          validator(value).map(OpenApiValidation.errorMessage)
        }
      }
      decoder
    }

    private def notSupported(value: String) = {
      throw new IllegalArgumentException(
        s"Cannot convert ZIO-Schema to XmlDecoder, because $value is currently not supported."
      )
    }

  }

}
