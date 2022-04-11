package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import com.sksamuel.avro4s.TypeUnionEntry._
import com.sksamuel.avro4s.TypeUnions._
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import org.apache.avro.util.Utf8

import scala.annotation.StaticAnnotation
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._

final case class AvroOneOf(default: Any) extends StaticAnnotation

final case class InvalidAvroOneOfDefaultValue(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

trait AvroOneOfUnknownSubtype {
  def withSubtype(subtype: String): this.type
}

private class EvolvableTypeUnionEncoder[T](
  ctx: SealedTrait[Encoder, T],
  val schemaFor: SchemaFor[T],
  encoderBySubtype: Map[Subtype[Encoder, T], UnionEncoder[T]#SubtypeEncoder],
  fieldNameBySubtype: Map[Subtype[Encoder, T], String]
) extends Encoder[T] {

  private val isEvolvable = findOneOfAnnotation(ctx.annotations).nonEmpty

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    if (!isEvolvable) {
      validateNewSchema(schemaFor)
    }
    TypeUnions.encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor))
  }

  private def encodeOneOf(value: T): AnyRef = {
    val schema = schemaFor.schema

    val (fieldName, encoded) = ctx.dispatch(value) { subtype =>
      fieldNameBySubtype(subtype) -> encoderBySubtype(subtype).encodeSubtype(value)
    }

    val oneOfRecordSchema = schema.getField(typeFieldName).schema()
    val oneOfFields = new Array[AnyRef](oneOfRecordSchema.getFields.size())
    oneOfFields(oneOfRecordSchema.getField(fieldName).pos()) = encoded
    val oneOfRecord = ImmutableRecord(oneOfRecordSchema, ArraySeq.unsafeWrapArray(oneOfFields))

    val recordFields = Array[AnyRef](fieldName, oneOfRecord)

    ImmutableRecord(schema, ArraySeq.unsafeWrapArray(recordFields))
  }

  private def encodeUnion(value: T): AnyRef = {
    ctx.dispatch(value)(subtype => encoderBySubtype(subtype).encodeSubtype(value))
  }

  def encode(value: T): AnyRef = {
    if (isEvolvable) {
      encodeOneOf(value)
    }
    else {
      encodeUnion(value)
    }
  }
}

private class EvolvableTypeUnionDecoder[T](
  ctx: SealedTrait[Decoder, T],
  val schemaFor: SchemaFor[T],
  decoderByName: Map[String, UnionDecoder[T]#SubtypeDecoder]
) extends Decoder[T] {

  private val maybeOneOfDefaultValue = {
    findOneOfAnnotation(ctx.annotations).map { case AvroOneOf(default) =>
      try {
        ctx.dispatch(default.asInstanceOf[T])(_ => default).asInstanceOf[T]
      }
      catch {
        case e: ClassCastException => throw InvalidAvroOneOfDefaultValue(e.getMessage, e)
        case e: IllegalArgumentException => throw InvalidAvroOneOfDefaultValue(e.getMessage, e)
      }
    }
  }

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    if (maybeOneOfDefaultValue.isEmpty) {
      validateNewSchema(schemaFor)
    }
    TypeUnions.decoder(ctx, new DefinitionEnvironment[Decoder](), FullSchemaUpdate(schemaFor))
  }

  private def createUnknownSubtypeValue(defaultValue: T, subtype: String) = {
    defaultValue match {
      case v: AvroOneOfUnknownSubtype => v.withSubtype(subtype)
      case v => v
    }
  }

  def decode(value: Any): T = {
    maybeOneOfDefaultValue match {
      case Some(defaultValue) =>
        decodeOneOf(value, defaultValue)
      case None =>
        decodeUnion(value)
    }
  }

  private def decodeOneOf(value: Any, defaultValue: T): T = {
    value match {
      case container: GenericRecord =>
        container.get(typeFieldName) match {
          case AvroStringMatcher(subtypeName) =>
            container.get(oneOfFieldName) match {
              case oneOfContainer: GenericRecord =>
                oneOfContainer.get(subtypeName) match {
                  case subtypeContainer: GenericContainer =>
                    val schemaName = subtypeContainer.getSchema.getFullName
                    val codecOpt = decoderByName.get(schemaName)

                    if (codecOpt.isDefined) {
                      codecOpt.get.decodeSubtype(subtypeContainer)
                    }
                    else {
                      createUnknownSubtypeValue(defaultValue, subtypeName)
                    }

                  case _ =>
                    createUnknownSubtypeValue(defaultValue, subtypeName)
                }

              case other =>
                throw new Avro4sDecodingException(
                  s"Expected a 'coproducts' field to be a GenericRecord, instead got $other (${other.getClass.getName}) in $value",
                  value,
                  this
                )
            }

          case other =>
            throw new Avro4sDecodingException(
              s"Expected a 'kind' field of type String, instead got $other (${other.getClass.getName}) in $value",
              value,
              this
            )
        }

      case _ =>
        throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value, this)
    }
  }

  private def decodeUnion(value: Any): T = value match {
    case container: GenericContainer =>
      val schemaName = container.getSchema.getFullName
      val codecOpt = decoderByName.get(schemaName)
      if (codecOpt.isDefined) {
        codecOpt.get.decodeSubtype(container)
      }
      else {
        val schemaNames = decoderByName.keys.toSeq.sorted.mkString("[", ", ", "]")
        throw new Avro4sDecodingException(
          s"Could not find schema $schemaName in type union schemas $schemaNames",
          value,
          this
        )
      }
    case _ => throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value, this)
  }
}

object TypeUnions {

  val typeFieldName = "type"
  val typeFieldDoc = ""
  val oneOfFieldName = "oneOf"
  val oneOfRecordSuffix = "_OneOf"
  val oneOfRecordDoc = ""

  object AvroStringMatcher {
    def unapply(value: AnyRef): Option[String] = {
      value match {
        case s: String => Some(s)
        case s: Utf8 => Some(s.toString)
        case _ => None
      }
    }
  }

  def toFieldName(fullName: String, namespace: String): String = {
    val truncated =
      if (fullName.startsWith(namespace + ".")) {
        fullName.drop(namespace.length + 1)
      }
      else {
        fullName
      }

    truncated.replaceAll("[^a-zA-Z0-9_]", "_")
  }

  private def createOneOfSchema[C[_], T](
    ctx: SealedTrait[C, T],
    nameExtractor: NameExtractor,
    subtypeSchemas: Seq[Schema]
  ) = {
    val flattened = subtypeSchemas.flatMap(schema => scala.util.Try(schema.getTypes.asScala).getOrElse(Seq(schema)))
    val (nulls, rest) = flattened.partition(_.getType == Schema.Type.NULL)
    val name = nameExtractor.name
    val namespace = nameExtractor.namespace
    val annotations = new AnnotationExtractors(ctx.annotations)

    val fields = (nulls.headOption.toSeq.view ++ rest).map { s: Schema =>
      new Schema.Field(
        toFieldName(s.getFullName, namespace),
        Schema.createUnion(s, Schema.create(Schema.Type.NULL)),
        ""
      )
    }.toList.asJava

    val oneOfRecord = Schema.createRecord(
      s"$name$oneOfRecordSuffix",
      oneOfRecordDoc,
      namespace,
      false,
      fields
    )

    val typeField = new Schema.Field(typeFieldName, Schema.create(Schema.Type.STRING), typeFieldDoc)
    val oneOfRecordField = new Schema.Field(oneOfFieldName, oneOfRecord, "")

    Schema.createRecord(
      name,
      annotations.doc.orNull,
      namespace,
      false,
      (typeField :: oneOfRecordField :: Nil).asJava
    )
  }

  private def findAnnotation[T: Manifest](annos: Seq[Any]): Option[T] = annos.collectFirst {
    case t: Any if manifest.runtimeClass.isAssignableFrom(t.getClass) => t.asInstanceOf[T]
  }

  def findOneOfAnnotation(annos: Seq[Any]): Option[AvroOneOf] =
    findAnnotation[AvroOneOf](annos)

  private def buildOneOfSchema[C[_], T](
    ctx: SealedTrait[C, T],
    nameExtractor: NameExtractor,
    update: SchemaUpdate,
    schemas: Seq[Schema]
  ): SchemaFor[T] = {
    update match {
      case FullSchemaUpdate(s) => s.forType
      case _ =>
        val annotations = ctx.annotations
        findAnnotation[AvroOneOf](annotations) match {
          case Some(_) =>
            SchemaFor(createOneOfSchema(ctx, nameExtractor, schemas), DefaultFieldMapper)

          case None =>
            SchemaFor(SchemaHelper.createSafeUnion(schemas: _*), DefaultFieldMapper)
        }
    }
  }

  def encoder[T](
    ctx: SealedTrait[Encoder, T],
    env: DefinitionEnvironment[Encoder],
    update: SchemaUpdate
  ): Encoder[T] = {
    // cannot extend the recursive environment with an initial type union encoder with empty union schema, as Avro Schema
    // doesn't support this. So we use the original recursive environment to build subtypes, meaning that in case of a
    // recursive schema, two identical type union encoders may be created instead of one.
    val subtypeEncoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionEncoder[T](st)(env, u) }

    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val namespace = nameExtractor.namespace
    val encoderBySubtype = subtypeEncoders.map(e => e.subtype -> e).toMap
    val fieldNameBySubtype = subtypeEncoders.map(e => e.subtype -> toFieldName(e.schema.getFullName, namespace)).toMap

    val schemaFor = findOneOfAnnotation(ctx.annotations) match {
      case Some(_) => buildOneOfSchema(ctx, nameExtractor, update, subtypeEncoders.map(_.schema))
      case None => buildSchema[T](update, subtypeEncoders.map(_.schema))
    }

    new EvolvableTypeUnionEncoder[T](ctx, schemaFor, encoderBySubtype, fieldNameBySubtype)
  }

  def decoder[T](
    ctx: SealedTrait[Decoder, T],
    env: DefinitionEnvironment[Decoder],
    update: SchemaUpdate
  ): Decoder[T] = {
    // cannot extend the recursive environment with an initial type union decoder with empty union schema, as Avro Schema
    // doesn't support this. So we use the original recursive environment to build subtypes, meaning that in case of a
    // recursive schema, two identical type union decoders may be created instead of one.
    val subtypeDecoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionDecoder[T](st)(env, u) }
    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val schemaFor = buildOneOfSchema(ctx, nameExtractor, update, subtypeDecoders.map(_.schema))
    val decoderByName = subtypeDecoders.map(decoder => decoder.fullName -> decoder).toMap
    new EvolvableTypeUnionDecoder[T](ctx, schemaFor, decoderByName)
  }

  def schema[T](
    ctx: SealedTrait[SchemaFor, T],
    env: DefinitionEnvironment[SchemaFor],
    update: SchemaUpdate
  ): SchemaFor[T] = {
    val subtypeSchemas = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionSchemaFor[T](st)(env, u) }
    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    buildOneOfSchema(ctx, nameExtractor, update, subtypeSchemas.map(_.schema))
  }

  private def enrichedSubtypes[Typeclass[_], T](
    ctx: SealedTrait[Typeclass, T],
    update: SchemaUpdate
  ): Seq[(Subtype[Typeclass, T], SchemaUpdate)] = {
    val enrichedUpdate = update match {
      case NoUpdate =>
        // in case of namespace annotations, pass the namespace update down to all subtypes
        val ns = new AnnotationExtractors(ctx.annotations).namespace
        ns.fold[SchemaUpdate](NoUpdate)(NamespaceUpdate)
      case _ => update
    }

    def subtypeSchemaUpdate(st: Subtype[Typeclass, T]) = enrichedUpdate match {
      case FullSchemaUpdate(schemaFor) =>
        val schema = schemaFor.schema
        val fieldMapper = schemaFor.fieldMapper
        val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)

        val subtypeSchema = findAnnotation[AvroOneOf](ctx.annotations) match {
          case Some(_) =>
            val fieldName = toFieldName(nameExtractor.fullName, schema.getNamespace)

            val maybeSubtypeSchema = for {
              subtype <- Option(schema.getField(typeFieldName))
              nullableField <- Option(subtype.schema().getField(fieldName))
              nullableSchema = nullableField.schema()
              types <- Option.when(nullableSchema.isUnion)(nullableSchema.getTypes)
              found <- types.asScala.find(!_.isNullable)
            } yield {
              SchemaFor(found, fieldMapper)
            }

            maybeSubtypeSchema.getOrElse(throw new Avro4sConfigurationException(
              s"Cannot find subtype schema for field '$fieldName' in schema: ${schema.toString(true)}"
            ))

          case None =>
            SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
        }

        FullSchemaUpdate(subtypeSchema)
      case _ => enrichedUpdate
    }

    def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    val sortedSubtypes = ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))

    sortedSubtypes.map(st => (st, subtypeSchemaUpdate(st)))
  }

  private[avro4s] def validateNewSchema[T](schemaFor: SchemaFor[T]) = {
    val newSchema = schemaFor.schema
    if (newSchema.getType != Schema.Type.UNION)
      throw new Avro4sConfigurationException(s"Schema type for record codecs must be UNION, received $newSchema")
  }

  def buildSchema[T](update: SchemaUpdate, schemas: Seq[Schema]): SchemaFor[T] = update match {
    case FullSchemaUpdate(s) => s.forType
    case _ => SchemaFor(SchemaHelper.createSafeUnion(schemas: _*), DefaultFieldMapper)
  }
}
