package dev.chopsticks.openapi.common

import dev.chopsticks.openapi.OpenApiParsedAnnotations
import zio.schema.{Schema, TypeId}
import zio.schema.TypeId.Nominal

object OpenApiConverterUtils {
  def getEntityName(schema: Schema[_]): Option[String] = {
    schema match {
      case r: Schema.Record[_] =>
        val parsed = OpenApiParsedAnnotations.extractAnnotations(r.annotations)
        getEntityName(Some(r.id), parsed)
      case r: Schema.Enum[_] =>
        val parsed = OpenApiParsedAnnotations.extractAnnotations(r.annotations)
        getEntityName(Some(r.id), parsed)
      case r: Schema.Lazy[_] =>
        getEntityName(r.schema)
      case other =>
        OpenApiParsedAnnotations.extractAnnotations(other.annotations).entityName
    }
  }

  private[chopsticks] def getCaseEntityName(
    schemaCase: Schema.Case[_, _],
    parsed: OpenApiParsedAnnotations[_]
  ): Option[String] = {
    getEntityName(schemaCase.schema, parsed)
  }

  private def getEntityName(schema: Schema[_], parsed: OpenApiParsedAnnotations[_]): Option[String] = {
    schema match {
      case r: Schema.Record[_] =>
        getEntityName(Some(r.id), parsed)
      case r: Schema.Lazy[_] =>
        getEntityName(r.schema, parsed)
      case _ =>
        parsed.entityName
    }
  }

  private[chopsticks] def getEntityName(typeId: Option[TypeId], metadata: OpenApiParsedAnnotations[_]) = {
    metadata.entityName.orElse {
      typeId match {
        case Some(Nominal(packageName, objectNames, typeName)) =>
          Some(OpenApiSettings.default.getTypeName(packageName, objectNames, typeName))
        case _ => None
      }
    }
  }

  private[chopsticks] def isSeq(schema: Schema[_]): Boolean = {
    schema match {
      case _: Schema.Sequence[_, _, _] => true
      case _: Schema.Set[_] => true
      case _: Schema.Primitive[_] => false
      case o: Schema.Optional[_] => isSeq(o.schema)
      case t: Schema.Transform[_, _, _] => isSeq(t.schema)
      case l: Schema.Lazy[_] => isSeq(l.schema)
      case _: Schema.Record[_] => false
      case _: Schema.Enum[_] => false
      case _: Schema.Map[_, _] => false
      case _: Schema.Either[_, _] => false
      case _: Schema.Tuple2[_, _] => false
      case _: Schema.Fail[_] => false
      case _: Schema.Fallback[_, _] =>
        throw new IllegalArgumentException("Fallback schema is not supported")
      case _: Schema.Dynamic =>
        throw new IllegalArgumentException("Dynamic schema is not supported")
    }
  }
}
