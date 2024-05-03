package dev.chopsticks.soap

import dev.chopsticks.openapi.common.{ConverterCache, OpenApiConverterUtils}
import dev.chopsticks.openapi.OpenApiSumTypeSerDeStrategy
import dev.chopsticks.soap.xsd.XsdAnnotations
import dev.chopsticks.soap.xsd.XsdAnnotations.extractAnnotations
import sttp.tapir.Validator
import zio.schema.{Schema, StandardType, TypeId}
import zio.Chunk

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.xml.{Elem, NodeSeq, TopScope, UnprefixedAttribute}

sealed trait XsdSimpleType {
  def name: String
}
object XsdSimpleType {
  final case object XsdBoolean extends XsdSimpleType {
    override def name: String = "xsd:boolean"
  }
  final case object XsdInteger extends XsdSimpleType {
    override def name: String = "xsd:integer"
  }
  final case object XsdString extends XsdSimpleType {
    override def name: String = "xsd:string"
  }
  final case object XsdHexBinary extends XsdSimpleType {
    override def name: String = "xsd:hexBinary"
  }
}

sealed trait XsdType extends Product with Serializable {
  def orderPriority: Int
}
object XsdType {
  final case object Simple extends XsdType {
    override def orderPriority: Int = 1
  }
  final case object Complex extends XsdType {
    override def orderPriority: Int = 2
  }
}

sealed trait XsdSchema[A] {
  def name: String
  def namespacedName(xsdNamespaceName: String): String
  def xsdTypes(xsdNamespaceName: String): ListMap[(XsdType, String), Elem] =
    collectXsdTypes(xsdNamespaceName, mutable.LinkedHashMap.empty).to(ListMap)
  private[chopsticks] def collectXsdTypes(
    xsdNamespaceName: String,
    acc: mutable.LinkedHashMap[(XsdType, String), Elem]
  ): mutable.LinkedHashMap[(XsdType, String), Elem]
}
object XsdSchema {
  def derive[A]()(implicit schema: Schema[A]): XsdSchema[A] = {
    new Converter().convert(schema, None)
  }

  // taken from tapir apispec-docs
  private def asPrimitiveValidators[A](v: Validator[A]): Seq[Validator.Primitive[_]] = {
    def toPrimitives(v: Validator[_]): Seq[Validator.Primitive[_]] = {
      v match {
        case Validator.Mapped(wrapped, _) => toPrimitives(wrapped)
        case Validator.All(validators) => validators.flatMap(toPrimitives)
        case Validator.Any(validators) => validators.flatMap(toPrimitives)
        case Validator.Custom(_, _) => Nil
        case bv: Validator.Primitive[_] => List(bv)
      }
    }
    toPrimitives(v)
  }

  private def collectPrimitiveConstraints[A](xs: Seq[Validator.Primitive[_]]) = {
    xs
      .foldLeft(NodeSeq.newBuilder) { (acc, v) =>
        v match {
          case Validator.Min(value, exclusive) =>
            acc.addOne(
              Elem(
                "xsd",
                if (exclusive) "minExclusive" else "minInclusive",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )
          case Validator.Max(value, exclusive) =>
            acc.addOne(
              Elem(
                "xsd",
                if (exclusive) "maxExclusive" else "maxInclusive",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )
          case Validator.MinLength(value, _) =>
            acc.addOne(
              Elem(
                "xsd",
                "minLength",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )
          case Validator.MaxLength(value, _) =>
            acc.addOne(
              Elem(
                "xsd",
                "maxLength",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )
          case Validator.Pattern(value) =>
            acc.addOne(
              Elem(
                "xsd",
                "pattern",
                new UnprefixedAttribute("value", value, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )

          case Validator.MaxSize(value) =>
            acc.addOne(
              Elem(
                "xsd",
                "maxLength",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )

          case Validator.MinSize(value) =>
            acc.addOne(
              Elem(
                "xsd",
                "minLength",
                new UnprefixedAttribute("value", value.toString, scala.xml.Null),
                TopScope,
                minimizeEmpty = true
              )
            )

          case Validator.Enumeration(possibleValues, encode, _) =>
            val encodeValue = encode
              .map(v => v.andThen(_.map(_.toString)))
              .getOrElse((v: Any) => Some(v.toString))
            possibleValues.foreach { v =>
              encodeValue(v) match {
                case None => ()
                case Some(encoded) =>
                  acc.addOne(
                    Elem(
                      "xsd",
                      "enumeration",
                      new UnprefixedAttribute("value", encoded, scala.xml.Null),
                      TopScope,
                      minimizeEmpty = true
                    )
                  )
              }
            }
            acc

          case Validator.Custom(_, _) => acc
        }
      }
      .result()
  }

  final case class Primitive[A](base: XsdSimpleType) extends XsdSchema[A] {
    override def namespacedName(xsdNamespaceName: String) = base.name
    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] = acc
    override def name: String = base.name
  }

  final case class LazyXsdSchema[A]() extends XsdSchema[A] with ConverterCache.Lazy[XsdSchema[A]] {
    def getSchema(): XsdSchema[A] = get
    override def namespacedName(xsdNamespaceName: String) = get.namespacedName(xsdNamespaceName)
    override def name: String = get.name
    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] =
      get.collectXsdTypes(xsdNamespaceName, acc)
  }

  final case class SimpleDerived[A](
    name: String,
    base: XsdSimpleType,
    validator: Validator[A]
  ) extends XsdSchema[A] {
    override def namespacedName(xsdNamespaceName: String): String = s"$xsdNamespaceName:$name"

    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] = {
      if (acc.contains((XsdType.Simple, name))) acc
      else {
        val primitiveValidators = asPrimitiveValidators(validator)
        val constraints = collectPrimitiveConstraints(primitiveValidators)
        val xmlElem = Elem(
          "xsd",
          "simpleType",
          new UnprefixedAttribute("name", name, scala.xml.Null),
          TopScope,
          minimizeEmpty = true,
          Elem(
            "xsd",
            "restriction",
            new UnprefixedAttribute("base", base.name, scala.xml.Null),
            TopScope,
            minimizeEmpty = true,
            constraints: _*
          )
        )
        acc += ((XsdType.Simple, name) -> xmlElem)
      }
    }
  }

  final case class Complex[A](
    name: String,
    validator: Validator[A],
    collectTypes: (
      String,
      String,
      mutable.LinkedHashMap[(XsdType, String), Elem]
    ) => mutable.LinkedHashMap[(XsdType, String), Elem]
  ) extends XsdSchema[A] {
    override def namespacedName(xsdNamespaceName: String): String = s"$xsdNamespaceName:$name"
    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] = {
      collectTypes(xsdNamespaceName, name, acc)
    }
  }

  final case class XsdOptional[A](underlying: XsdSchema[A]) extends XsdSchema[A] {
    override def name: String = underlying.name

    override def namespacedName(xsdNamespaceName: String): String = underlying.namespacedName(xsdNamespaceName)
    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] = {
      underlying.collectXsdTypes(xsdNamespaceName, acc)
    }
  }

  final case class XsdSequence[A](underlying: XsdSchema[A], nodeName: String, validator: Validator[A])
      extends XsdSchema[A] {
    override def name: String = underlying.name
    override def namespacedName(xsdNamespaceName: String): String = underlying.namespacedName(xsdNamespaceName)

    override private[chopsticks] def collectXsdTypes(
      xsdNamespaceName: String,
      acc: mutable.LinkedHashMap[(XsdType, String), Elem]
    ): mutable.LinkedHashMap[(XsdType, String), Elem] = {
      underlying.collectXsdTypes(xsdNamespaceName, acc)
    }
  }

  private val _boolSchemaType = Primitive[Boolean](XsdSimpleType.XsdBoolean)
  private val _intSchemaType = Primitive[Int](XsdSimpleType.XsdInteger)
  private val _stringSchemaType = Primitive[String](XsdSimpleType.XsdString)
  private val _hexBinarySchemaType = Primitive[Array[Byte]](XsdSimpleType.XsdHexBinary)

  private def boolSchema[A] = _boolSchemaType.asInstanceOf[Primitive[A]]
  private def intSchema[A] = _intSchemaType.asInstanceOf[Primitive[A]]
  private def stringSchema[A] = _stringSchemaType.asInstanceOf[Primitive[A]]
  private def hexBinarySchema[A] = _hexBinarySchemaType.asInstanceOf[Primitive[A]]

  private class Converter(cache: ConverterCache[XsdSchema] = new ConverterCache[XsdSchema]) {
    private def convertUsingCache[A](
      typeId: TypeId,
      annotations: XsdAnnotations[A]
    )(convert: => XsdSchema[A]): XsdSchema[A] = {
      cache
        .convertUsingCache(typeId, annotations.xmlAnnotations.openApiAnnotations)(convert) { () =>
          new LazyXsdSchema[A]()
        }
    }

    private[chopsticks] def convert[A](schema: Schema[A], xmlSeqNodeName: Option[String]): XsdSchema[A] = {
      schema match {
        case Schema.Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, _, _, annotations, _) =>
          val parsed = extractAnnotations[A](annotations)
          val nodeName = parsed.xmlAnnotations.xmlFieldName
            .orElse(xmlSeqNodeName)
            .getOrElse {
              throw new RuntimeException("Sequence must have xmlFieldName annotation")
            }
          addAnnotations[A](
            None,
            XsdSequence(convert(schemaA, Some(nodeName)), nodeName, Validator.pass).asInstanceOf[XsdSchema[A]],
            parsed
          )

        case Schema.Set(_, _) =>
          ???

        case Schema.Transform(schema, _, _, annotations, _) =>
          val typedAnnotations = extractAnnotations[A](annotations)
          val baseEncoder = convert(schema, typedAnnotations.xmlAnnotations.xmlFieldName.orElse(xmlSeqNodeName))
            .asInstanceOf[XsdSchema[A]]
          addAnnotations(None, baseEncoder, typedAnnotations)

        case Schema.Optional(schema, annotations) =>
          addAnnotations[A](
            None,
            baseSchema = XsdOptional(convert(schema, xmlSeqNodeName)).asInstanceOf[XsdSchema[A]],
            metadata = extractAnnotations(annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema, xmlSeqNodeName)

        case s: Schema.Record[A] =>
          convertRecord[A](s)

        case s: Schema.Enum[A] =>
          convertEnum[A](s)

        case other =>
          throw new IllegalArgumentException(s"Unsupported schema type: $other")
      }
    }

    private def findChildTypeName(namespace: String, xsdSchema: XsdSchema[_]): String = xsdSchema match {
      case o: XsdOptional[_] => findChildTypeName(namespace, o.underlying)
      case l: LazyXsdSchema[_] => findChildTypeName(namespace, l.getSchema())
      case _: Primitive[_] => xsdSchema.name
      case _ => s"$namespace:${xsdSchema.name}"
    }

    private def convertRecord[A](r: Schema.Record[A]): XsdSchema[A] = {

      def isOptional(s: XsdSchema[_]): Boolean = s match {
        case _: XsdOptional[_] => true
        case xs: XsdSequence[_] => isOptional(xs.underlying)
        case _ => false
      }

      def minOccurs(s: XsdSchema[_]): Option[Int] = {
        s match {
          case _: XsdOptional[_] => Some(0)
          case xs: XsdSequence[_] =>
            asPrimitiveValidators(xs.validator).collectFirst {
              case Validator.MinSize(value) => value
            }
          case _ => None
        }
      }

      def maxOccurs(s: XsdSchema[_]): Option[Int] = s match {
        case o: XsdOptional[_] => maxOccurs(o.underlying)
        case xs: XsdSequence[_] =>
          asPrimitiveValidators(xs.validator).collectFirst {
            case Validator.MaxSize(value) => value
          }
        case _ => None
      }

      def asSeq(s: XsdSchema[_]): Option[XsdSequence[_]] = s match {
        case o: XsdOptional[_] => asSeq(o.underlying)
        case s: XsdSequence[_] => Some(s)
        case _ => None
      }

      val recordAnnotations: XsdAnnotations[A] = extractAnnotations[A](r.annotations)
      convertUsingCache(r.id, recordAnnotations) {
        val fieldEncoders = r.fields
          .map { field =>
            val fieldAnnotations = extractAnnotations[Any](field.annotations)
            addAnnotations[Any](
              None,
              convert[Any](
                field.schema.asInstanceOf[Schema[Any]],
                fieldAnnotations.xmlAnnotations.xmlFieldName.orElse(Some(field.name))
              ),
              fieldAnnotations
            )
          }
        val fieldNames = r.fields.map { field =>
          extractAnnotations[Any](field.annotations).xmlAnnotations.xmlFieldName
        }

        val baseEncoder = Complex[A](
          name = "", // placeholder, will be replaced by the addAnnotations
          validator = Validator.pass,
          collectTypes = (namespace, name, acc) => {
            var result = acc
            val fieldsBuilder = NodeSeq.newBuilder
            var i = 0
            while (i < r.fields.length) {
              val field = r.fields(i)
              val encoder = fieldEncoders(i)
              result = encoder.collectXsdTypes(namespace, result)
              val childTypeName = findChildTypeName(namespace, encoder)
              val fieldName = fieldNames(i).getOrElse(field.name)
              val encoderAsSeq = asSeq(encoder)
              val isSequence = encoderAsSeq.isDefined
              val maxOccursTag = {
                if (isSequence) {
                  val max = maxOccurs(encoder)
                    .map(_.toString)
                    .getOrElse("unbounded")
                  new UnprefixedAttribute("maxOccurs", max, scala.xml.Null)
                }
                else {
                  scala.xml.Null
                }
              }

              val minOccursTag = {
                Option
                  .when(isOptional(encoder))(0)
                  .orElse(minOccurs(encoder)) match {
                  case Some(min) => new UnprefixedAttribute("minOccurs", min.toString, maxOccursTag)
                  case None => maxOccursTag
                }
              }

              val newElem = scala.xml.Elem(
                "xsd",
                "element",
                new UnprefixedAttribute(
                  "name",
                  encoderAsSeq.map(_.nodeName).getOrElse(fieldName),
                  new UnprefixedAttribute(
                    "type",
                    childTypeName,
                    minOccursTag
                  )
                ),
                TopScope,
                minimizeEmpty = true
              )
              val _ = fieldsBuilder.addOne(newElem)
              i += 1
            }
            val fieldElems = fieldsBuilder.result()

            val complexTypeElem = Elem(
              "xsd",
              "complexType",
              new UnprefixedAttribute("name", name, scala.xml.Null),
              TopScope,
              minimizeEmpty = true,
              Elem(
                "xsd",
                "sequence",
                scala.xml.Null,
                TopScope,
                minimizeEmpty = true,
                fieldElems: _*
              )
            )
            result += ((XsdType.Complex, name) -> complexTypeElem)
          }
        )
        addAnnotations(Some(r.id), baseEncoder, recordAnnotations)
      }
    }

    private def convertEnum[A](s: Schema.Enum[A]): XsdSchema[A] = {
      val enumAnnotations = extractAnnotations[A](s.annotations)
      val serDeStrategy = enumAnnotations.xmlAnnotations.openApiAnnotations.sumTypeSerDeStrategy
        .getOrElse {
          throw new RuntimeException(
            s"Discriminator must be defined to derive an XsdSchema. Received annotations: $enumAnnotations"
          )
        }
      serDeStrategy match {
        case OpenApiSumTypeSerDeStrategy.Discriminator(discriminator) =>
          val reversedDiscriminator = discriminator.mapping.map(_.swap)
          if (reversedDiscriminator.size != discriminator.mapping.size) {
            throw new RuntimeException(
              s"Cannot derive XsdSchema for ${s.id.name}, because discriminator mapping is not unique."
            )
          }
          val schemasByDiscType = s.cases.iterator
            .map { c =>
              val schema = addAnnotations(
                None,
                convert(c.schema, None),
                extractAnnotations(c.annotations)
              )
              reversedDiscriminator(c.caseName) -> (schema, c)
            }
            .toMap
          val diff = discriminator.mapping.keySet.diff(schemasByDiscType.keySet)
          if (diff.nonEmpty) {
            throw new RuntimeException(
              s"Cannot derive XsdSchema for ${s.id.name}, because mapping and decoders don't match. Diff=$diff."
            )
          }
          val schema =
            Complex[A](
              name = "",
              validator = Validator.pass,
              collectTypes = (namespace, name, acc) => {
                var result = acc
                result = schemasByDiscType.iterator.foldLeft(result) { case (a, (_, (s, _))) =>
                  s.collectXsdTypes(namespace, a)
                }
                val subtypesDefinition = schemasByDiscType.iterator
                  .foldLeft(NodeSeq.newBuilder) { case (builder, (tpe, (schema, _))) =>
                    builder += Elem(
                      "xsd",
                      "element",
                      new UnprefixedAttribute(
                        "name",
                        tpe,
                        new UnprefixedAttribute("type", findChildTypeName(namespace, schema), scala.xml.Null)
                      ),
                      TopScope,
                      minimizeEmpty = true
                    )
                  }
                  .result()
                val xmlDefinition = Elem(
                  "xsd",
                  "complexType",
                  new UnprefixedAttribute("name", name, scala.xml.Null),
                  TopScope,
                  minimizeEmpty = true,
                  Elem(
                    "xsd",
                    "choice",
                    scala.xml.Null,
                    TopScope,
                    minimizeEmpty = true,
                    subtypesDefinition: _*
                  )
                )
                result += ((XsdType.Complex, name) -> xmlDefinition)
              }
            )

          addAnnotations(Some(s.id), schema, enumAnnotations)
      }

    }

    private def primitiveConverter[A](
      standardType: StandardType[A],
      annotations: Chunk[Any]
    ): XsdSchema[A] = {
      val baseEncoder = standardType match {
//        case StandardType.UnitType => unitEncoder
        case StandardType.StringType => stringSchema[A]
        case StandardType.BoolType => boolSchema[A]
//        case StandardType.ByteType => byteEncoder
//        case StandardType.ShortType =>
        case StandardType.IntType => intSchema[A]
        case StandardType.BinaryType => hexBinarySchema[A]
        case other =>
          throw new IllegalArgumentException(s"Unsupported standard type: $other")
      }
      addAnnotations(None, baseEncoder.asInstanceOf[XsdSchema[A]], extractAnnotations(annotations))
    }

    private def addAnnotations[A](
      typeId: Option[TypeId],
      baseSchema: XsdSchema[A],
      metadata: XsdAnnotations[A]
    ): XsdSchema[A] = {
      val name = metadata
        .xsdSchemaName
        .orElse(OpenApiConverterUtils.getEntityName(typeId, metadata.xmlAnnotations.openApiAnnotations))
      // todo take default into account?
      baseSchema match {
        case Primitive(base) =>
          metadata.xmlAnnotations.openApiAnnotations.validator match {
            case Some(_) if name.isEmpty =>
              throw new IllegalArgumentException(
                "xsdSchemaName or entityName annotation is required for simple types with validator"
              )
            case Some(validator) =>
              SimpleDerived(name.get, base, validator)
            case None if name.isEmpty => baseSchema
            case None => SimpleDerived(name.get, base, Validator.pass)
          }
        case d: SimpleDerived[A] =>
          var result = d
          result = metadata.xmlAnnotations.openApiAnnotations.validator match {
            case Some(v) =>
              if (result.validator == Validator.pass) result.copy(validator = v)
              else result.copy(validator = result.validator.and(v))
            case None => result
          }
          result = name match {
            case Some(value) => result.copy(name = value)
            case None => result
          }
          result
        case c: Complex[A] =>
          var result = c
          result = metadata.xmlAnnotations.openApiAnnotations.validator match {
            case Some(v) =>
              if (result.validator == Validator.pass) result.copy(validator = v)
              else result.copy(validator = result.validator.and(v))
            case None => result
          }
          result = name match {
            case Some(value) => result.copy(name = value)
            case None => result
          }
          result
        case XsdOptional(_) => baseSchema
        case xs: XsdSequence[A] =>
          var result = xs
          result = metadata.xmlAnnotations.openApiAnnotations.validator match {
            case Some(v) =>
              if (result.validator == Validator.pass) result.copy(validator = v)
              else result.copy(validator = result.validator.and(v))
            case None => result
          }
          result
        case LazyXsdSchema() => ???
      }
    }

  }

}
