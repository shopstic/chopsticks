package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import com.sksamuel.avro4s.TypeUnionEntry._
import com.sksamuel.avro4s.TypeUnions._
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecord}

import scala.annotation.StaticAnnotation
import scala.jdk.CollectionConverters._

final case class AvroEvolvableUnion(default: Any) extends StaticAnnotation

final case class InvalidAvroEvolvableUnionDefaultValue(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

private class EvolvableTypeUnionEncoder[T](
  ctx: SealedTrait[Encoder, T],
  val schemaFor: SchemaFor[T],
  encoderBySubtype: Map[Subtype[Encoder, T], UnionEncoder[T]#SubtypeEncoder],
  fieldNameBySubtype: Map[Subtype[Encoder, T], String]
) extends Encoder[T] {

  private val isEvolvable = findEvolvableUnionAnnotation(ctx.annotations).nonEmpty

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    if (!isEvolvable) {
      validateNewSchema(schemaFor)
    }
    TypeUnions.encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor))
  }

  private def encodeEvolvable(value: T): AnyRef = {
    val schema = schemaFor.schema
    val record = new GenericData.Record(schema)

    val (fieldName, encoded) = ctx.dispatch(value) { subtype =>
      fieldNameBySubtype(subtype) -> encoderBySubtype(subtype).encodeSubtype(value)
    }

    val coproducts = new GenericData.Record(schema.getField("coproducts").schema())
    coproducts.put(fieldName, encoded)

    record.put("kind", fieldName)
    record.put("coproducts", coproducts)
    record
  }

  private def encodeNative(value: T): AnyRef = {
    ctx.dispatch(value)(subtype => encoderBySubtype(subtype).encodeSubtype(value))
  }

  def encode(value: T): AnyRef = {
    if (isEvolvable) {
      encodeEvolvable(value)
    }
    else {
      encodeNative(value)
    }
  }
}

private class EvolvableTypeUnionDecoder[T](
  ctx: SealedTrait[Decoder, T],
  val schemaFor: SchemaFor[T],
  decoderByName: Map[String, UnionDecoder[T]#SubtypeDecoder]
) extends Decoder[T] {

  private val evolvableUnionDefaultValue = {
    findEvolvableUnionAnnotation(ctx.annotations).map { case AvroEvolvableUnion(default) =>
      try {
        ctx.dispatch(default.asInstanceOf[T])(_ => default)
      }
      catch {
        case e: ClassCastException => throw InvalidAvroEvolvableUnionDefaultValue(e.getMessage, e)
        case e: IllegalArgumentException => throw InvalidAvroEvolvableUnionDefaultValue(e.getMessage, e)
      }
    }
  }

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    if (evolvableUnionDefaultValue.isEmpty) {
      validateNewSchema(schemaFor)
    }
    TypeUnions.decoder(ctx, new DefinitionEnvironment[Decoder](), FullSchemaUpdate(schemaFor))
  }

  def decode(value: Any): T = {
    evolvableUnionDefaultValue match {
      case Some(defaultValue) =>
        decodeEvolvable(value, defaultValue)
      case None =>
        decodeNative(value)
    }
  }

  private def decodeEvolvable(value: Any, defaultValue: Any): T = {
    value match {
      case container: GenericRecord =>
        container.get("kind") match {
          case kind: String =>
            container.get("coproducts") match {
              case nestedContainer: GenericRecord =>
                nestedContainer.get(kind) match {
                  case subtypeContainer: GenericContainer =>
                    val schemaName = subtypeContainer.getSchema.getFullName
                    val codecOpt = decoderByName.get(schemaName)

                    if (codecOpt.isDefined) {
                      codecOpt.get.decodeSubtype(subtypeContainer)
                    }
                    else {
                      defaultValue.asInstanceOf[T]
                    }

                  case _ =>
                    defaultValue.asInstanceOf[T]
                }

              case other =>
                throw new Avro4sDecodingException(
                  s"Expected a 'coproducts' field to be a GenericRecord, instead got $other in $value",
                  value,
                  this
                )
            }

          case other =>
            throw new Avro4sDecodingException(
              s"Expected a 'kind' field of type String, instead got $other in $value",
              value,
              this
            )
        }

      case _ =>
        throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value, this)
    }
  }

  private def decodeNative(value: Any): T = value match {
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

  def toFieldName(schema: Schema, namespace: String): String = {
    val fullName = schema.getFullName

    val truncated =
      if (fullName.startsWith(namespace + ".")) {
        fullName.drop(namespace.length + 1)
      }
      else {
        fullName
      }

    truncated.replaceAll("[^a-zA-Z0-9_]", "_")
  }

  private def createEvolvableUnion[C[_], T](
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
        toFieldName(s, namespace),
        Schema.createUnion(s, Schema.create(Schema.Type.NULL)),
        ""
      )
    }.toList.asJava

    val coproductsRecord = Schema.createRecord(
      s"${name}_Coproducts",
      "",
      namespace,
      false,
      fields
    )

    val kindField = new Schema.Field("kind", Schema.create(Schema.Type.STRING), "")
    val coproductsField = new Schema.Field("coproducts", coproductsRecord, "")

    Schema.createRecord(
      name,
      annotations.doc.orNull,
      namespace,
      false,
      (kindField :: coproductsField :: Nil).asJava
    )
  }

  private def findAnnotation[T: Manifest](annos: Seq[Any]): Option[T] = annos.collectFirst {
    case t: Any if manifest.runtimeClass.isAssignableFrom(t.getClass) => t.asInstanceOf[T]
  }

  def findEvolvableUnionAnnotation(annos: Seq[Any]): Option[AvroEvolvableUnion] =
    findAnnotation[AvroEvolvableUnion](annos)

  private def buildEvolvableSchema[C[_], T](
    ctx: SealedTrait[C, T],
    nameExtractor: NameExtractor,
    update: SchemaUpdate,
    schemas: Seq[Schema]
  ): SchemaFor[T] = {
    update match {
      case FullSchemaUpdate(s) => s.forType
      case _ =>
        val annotations = ctx.annotations
        findAnnotation[AvroEvolvableUnion](annotations) match {
          case Some(_) =>
            SchemaFor(createEvolvableUnion(ctx, nameExtractor, schemas), DefaultFieldMapper)

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
    val fieldNameBySubtype = subtypeEncoders.map(e => e.subtype -> toFieldName(e.schema, namespace)).toMap

    val schemaFor = findEvolvableUnionAnnotation(ctx.annotations) match {
      case Some(_) => buildEvolvableSchema(ctx, nameExtractor, update, subtypeEncoders.map(_.schema))
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
    val schemaFor = buildEvolvableSchema(ctx, nameExtractor, update, subtypeDecoders.map(_.schema))
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
    buildEvolvableSchema(ctx, nameExtractor, update, subtypeSchemas.map(_.schema))
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
        val subtypeSchema = SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
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
