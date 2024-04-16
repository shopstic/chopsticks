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
}
