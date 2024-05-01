package dev.chopsticks.xml

import dev.chopsticks.openapi.common.{ConverterCache, OpenApiConverterUtils}
import dev.chopsticks.openapi.OpenApiSumTypeSerDeStrategy
import dev.chopsticks.util.Hex
import dev.chopsticks.xml.XmlAnnotations.extractAnnotations

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
import scala.xml.{Elem, NodeSeq, TopScope}
import zio.Chunk
import zio.schema.{Schema, StandardType, TypeId}
import zio.schema.Schema.{Field, Primitive}

import scala.annotation.nowarn

trait XmlEncoder[A] { self =>
  def encode(a: A): NodeSeq
  def isOptional: Boolean
  final def contramap[B](f: B => A): XmlEncoder[B] = new XmlEncoder[B] {
    override def isOptional: Boolean = self.isOptional
    override def encode(value: B): NodeSeq = {
      self.encode(f(value))
    }
  }
}
object XmlEncoder {
  def derive[A]()(implicit schema: Schema[A]): XmlEncoder[A] = {
    new Converter().convert(schema, None)
  }

  private def createPrimitiveEncoder[A](f: A => scala.xml.NodeSeq): XmlEncoder[A] = new XmlEncoder[A] {
    final override def isOptional: Boolean = false
    override def encode(a: A): NodeSeq = f(a)
  }

  private val emptyNode = scala.xml.Text("")

  private val unitEncoder: XmlEncoder[Unit] = createPrimitiveEncoder(_ => emptyNode)
  private val boolEncoder: XmlEncoder[Boolean] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val byteEncoder: XmlEncoder[Byte] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val shortEncoder: XmlEncoder[Short] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val intEncoder: XmlEncoder[Int] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val longEncoder: XmlEncoder[Long] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val floatEncoder: XmlEncoder[Float] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val doubleEncoder: XmlEncoder[Double] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val stringEncoder: XmlEncoder[String] = createPrimitiveEncoder(x => scala.xml.Text(x))
  private val charEncoder: XmlEncoder[Char] = createPrimitiveEncoder(x => scala.xml.Text(String.valueOf(x)))
  private val binaryViaHexEncoder: XmlEncoder[Chunk[Byte]] =
    createPrimitiveEncoder(x => scala.xml.Text(Hex.encode(x.toArray)))
  private val instantEncoder: XmlEncoder[Instant] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val offsetDateTimeEncoder: XmlEncoder[OffsetDateTime] =
    createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val zonedDateTimeEncoder: XmlEncoder[ZonedDateTime] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val localDateTimeEncoder: XmlEncoder[LocalDateTime] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val localDateEncoder: XmlEncoder[LocalDate] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val localTimeEncoder: XmlEncoder[LocalTime] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val offsetTimeEncoder: XmlEncoder[OffsetTime] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val zoneIdEncoder: XmlEncoder[ZoneId] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val zoneOffsetEncoder: XmlEncoder[ZoneOffset] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val bigDecimalEncoder: XmlEncoder[BigDecimal] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val bigIntEncoder: XmlEncoder[BigInt] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val uuidEncoder: XmlEncoder[UUID] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val dayOfWeekEncoder: XmlEncoder[DayOfWeek] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))
  private val durationEncoder: XmlEncoder[Duration] = createPrimitiveEncoder(x => scala.xml.Text(x.toString))

  private def encodeOption[A](underlying: XmlEncoder[A]): XmlEncoder[Option[A]] = new XmlEncoder[Option[A]] {
    final override def isOptional: Boolean = true
    override def encode(a: Option[A]): NodeSeq = a match {
      case Some(x) => underlying.encode(x)
      case None => scala.xml.NodeSeq.Empty
    }
  }

  private def encodeChunk[A](underlying: XmlEncoder[A], nodeName: String): XmlEncoder[Chunk[A]] =
    new XmlEncoder[Chunk[A]] {
      final override def isOptional: Boolean = false
      override def encode(xs: Chunk[A]): NodeSeq = {
        val builder = scala.xml.NodeSeq.newBuilder
        var i = 0
        while (i < xs.length) {
          val childNodes = underlying.encode(xs(i))
          val _ =
            builder += scala.xml.Elem(null, nodeName, scala.xml.Null, TopScope, minimizeEmpty = true, childNodes: _*)
          i += 1
        }
        builder.result()
      }
    }

  final private[xml] class LazyEncoder[A] extends XmlEncoder[A] with ConverterCache.Lazy[XmlEncoder[A]] {
    override def isOptional: Boolean = get.isOptional
    override def encode(a: A): NodeSeq = {
      get.encode(a)
    }
  }

  private class Converter(
    cache: ConverterCache[XmlEncoder] = new ConverterCache[XmlEncoder]()
  ) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: XmlAnnotations[A]
    )(convert: => XmlEncoder[A]): XmlEncoder[A] = {
      cache.convertUsingCache(typeId, annotations.openApiAnnotations)(convert)(() => new LazyEncoder[A]())
    }

    // scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
    def convert[A](schema: Schema[A], fieldName: Option[String]): XmlEncoder[A] = {
      schema match {
        case Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, _, toChunk, annotations, _) =>
          val parsed = extractAnnotations[A](annotations)
          val nodeName = parsed.xmlFieldName
            .orElse(fieldName)
            .getOrElse {
              throw new RuntimeException("Sequence must have xmlFieldName annotation")
            }
          addAnnotations(
            None,
            encodeChunk(convert(schemaA, Some(nodeName)), nodeName).contramap(toChunk),
            parsed
          )

        case Schema.Set(_, _) =>
          ???

        case Schema.Transform(schema, _, g, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseEncoder = convert(schema, typedAnnotations.xmlFieldName.orElse(fieldName)).contramap[A] { x =>
            g(x) match {
              case Right(v) => v
              case Left(error) => throw new RuntimeException(s"Couldn't transform schema: $error")
            }
          }
          addAnnotations(None, baseEncoder, typedAnnotations)

        case Schema.Optional(schema, annotations) =>
          val parsed = extractAnnotations[A](annotations)
          addAnnotations[A](
            None,
            baseEncoder = encodeOption(convert(schema, parsed.xmlFieldName.orElse(fieldName))).asInstanceOf[XmlEncoder[A]],
            metadata = extractAnnotations(annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema, fieldName)

        case s: Schema.Record[A] =>
          convertRecord[A](s.id, s.annotations, s.fields)

        case s: Schema.Enum[A] =>
          convertEnum[A](s)

        case _ =>
          ???

      }
    }

    private def convertEnum[A](schema: Schema.Enum[A]): XmlEncoder[A] = {
      val enumAnnotations = extractAnnotations[A](schema.annotations)
      val serDeStrategy = enumAnnotations.openApiAnnotations.sumTypeSerDeStrategy
        .getOrElse {
          throw new RuntimeException(
            s"Discriminator must be defined to derive an XmlEncoder. Received annotations: $enumAnnotations"
          )
        }

      serDeStrategy match {
        case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
          if (discriminator.mapping.size != schema.cases.size) {
            throw new RuntimeException(
              s"Cannot derive XmlEncoder for ${schema.id.name}, because discriminator mapping has different length than the number of cases. Discriminator mapping length = ${discriminator.mapping.size}, possible cases: ${schema.cases.map(_.caseName).mkString(", ")}."
            )
          }
          val reversedDiscriminator = discriminator.mapping.map(_.swap)
          if (reversedDiscriminator.size != discriminator.mapping.size) {
            throw new RuntimeException(
              s"Cannot derive XmlEncoder for ${schema.id.name}, because discriminator mapping is not unique."
            )
          }
          val encoderByDiscValue = {
            schema.cases.iterator
              .map { c =>
                val encoder = addAnnotations(
                  None,
                  convert(c.schema, None),
                  extractAnnotations(c.annotations)
                ).asInstanceOf[XmlEncoder[Any]]
                reversedDiscriminator(c.caseName) -> encoder
              }
              .toMap
          }

          convertUsingCache(schema.id, enumAnnotations) {
            val baseDecoder = new XmlEncoder[A] {
              final override def isOptional: Boolean = false
              override def encode(a: A): NodeSeq = {
                val discriminatorValue = discriminator.discriminatorValue(a)
                val encoder = encoderByDiscValue(discriminatorValue)
                Elem(
                  null,
                  discriminatorValue,
                  scala.xml.Null,
                  TopScope,
                  minimizeEmpty = true,
                  encoder.encode(a): _*
                )
              }
            }
            addAnnotations(Some(schema.id), baseDecoder, enumAnnotations)
          }

      }

    }

    private def convertRecord[A](id: TypeId, annotations: Chunk[Any], fields: Chunk[Field[A, _]]): XmlEncoder[A] = {
      val recordAnnotations: XmlAnnotations[A] = extractAnnotations[A](annotations)
      convertUsingCache(id, recordAnnotations) {
        val fieldEncoders = fields
          .map { field =>
            val fieldAnnotations = extractAnnotations[Any](field.annotations)
            addAnnotations[Any](
              None,
              convert[Any](
                field.schema.asInstanceOf[Schema[Any]],
                fieldAnnotations.xmlFieldName.orElse(Some(field.name))
              ),
              fieldAnnotations
            )
          }
        val fieldNames = fields.map { field => extractAnnotations[Any](field.annotations).xmlFieldName }
        val isFieldSeq = fields.map { field => OpenApiConverterUtils.isSeq(field.schema) }
        val baseEncoder = new XmlEncoder[A] {
          final override def isOptional: Boolean = false
          override def encode(value: A): NodeSeq = {
            val builder = NodeSeq.newBuilder
            var i = 0
            while (i < fields.length) {
              val field = fields(i)
              val encoder = fieldEncoders(i)
              val isSeq = isFieldSeq(i)
              val childNodes = encoder
                .asInstanceOf[XmlEncoder[Any]]
                .encode(field.get(value))
              if (!(childNodes.isEmpty && encoder.isOptional)) {
                if (!isSeq) {
                  val fieldName = fieldNames(i).getOrElse(field.name)
                  val newElem = scala.xml.Elem(
                    null,
                    fieldName,
                    scala.xml.Null,
                    TopScope,
                    minimizeEmpty = true,
                    childNodes: _*
                  )
                  val _ = builder.addOne(newElem)
                }
                else {
                  val _ = builder.addAll(childNodes)
                }
              }
              i += 1
            }
            builder.result()
          }
        }
        addAnnotations(Some(id), baseEncoder, recordAnnotations)
      }
    }

//    private def convertEnum[A](
//      id: TypeId,
//      annotations: Chunk[Any],
//      cases: Chunk[Schema.Case[A, _]]
//    ): XmlEncoder[A] = {
//      val enumAnnotations = extractAnnotations[A](annotations)
//      val encodersByName = cases.iterator
//        .map { c =>
//          val cAnn = extractAnnotations(c.annotations)
//          val encoder = addAnnotations(
//            None,
//            convert(c.schema),
//            extractAnnotations(c.annotations)
//          ).asInstanceOf[XmlEncoder[Any]]
//          val entityName = OpenApiConverterUtils.getCaseEntityName(c, cAnn).getOrElse(throw new RuntimeException(
//            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined or be a case class to derive a XmlEncoder. Received annotations: $cAnn"
//          ))
//          entityName -> (encoder, c)
//        }
//        .toMap
//      val discriminator = enumAnnotations.sumTypeSerDeStrategy
//
//      val decoder = discriminator
//        .getOrElse(throw new RuntimeException(
//          s"Discriminator must be defined to derive an XmlEncoder. Received annotations: $enumAnnotations"
//        )) match {
//        case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
//          val diff = discriminator.mapping.values.toSet.diff(encodersByName.keySet)
//          if (diff.nonEmpty) {
//            throw new RuntimeException(
//              s"Cannot derive CsvEncoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and decoders don't match. Diff=$diff."
//            )
//          }
//          new XmlEncoder[A] {
//            override def encode(
//              value: A,
//              columnName: Option[String],
//              acc: mutable.LinkedHashMap[String, String]
//            ): mutable.LinkedHashMap[String, String] = {
//              var res = acc
//              val discValue = discriminator.discriminatorValue(value)
//              val (enc, c) = encodersByName(discriminator.mapping(discValue))
//              val discriminatorColumnName =
//                Some(options.nestedFieldLabel(columnName, discriminator.discriminatorFieldName))
//              res = stringEncoder.encode(discriminator.discriminatorFieldName, discriminatorColumnName, res)
//              enc.encode(c.deconstruct(value).asInstanceOf[Any], columnName, res)
//            }
//          }
//      }
//      addAnnotations(Some(id), decoder, enumAnnotations)
//    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): XmlEncoder[A] = {
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
        case StandardType.BinaryType => binaryViaHexEncoder
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
      addAnnotations(None, baseEncoder.asInstanceOf[XmlEncoder[A]], extractAnnotations(annotations))
    }

    @nowarn("cat=unused-params")
    private def addAnnotations[A](
      typeId: Option[TypeId],
      baseEncoder: XmlEncoder[A],
      metadata: XmlAnnotations[A]
    ): XmlEncoder[A] = {
      baseEncoder
    }

    private def notSupported(value: String) = {
      throw new IllegalArgumentException(
        s"Cannot convert ZIO-Schema to XmlEncoder, because $value is currently not supported."
      )
    }

  }
}
