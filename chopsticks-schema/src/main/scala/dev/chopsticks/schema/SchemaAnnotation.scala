package dev.chopsticks.schema

import sttp.tapir.Validator
import zio.Chunk
import zio.schema.TypeId

import scala.annotation.StaticAnnotation

enum SchemaAnnotation extends StaticAnnotation:
  case EntityName(name: String)
  case Default[A](value: A, encodedValue: Option[Any] = None)
  case Description(text: String)
  case Validate[A](validator: Validator[A])
  case SumTypeSerDeStrategy[A](value: SchemaSumTypeSerDeStrategy[A])
  case JsonCaseConverter(from: SchemaNamingConvention, to: SchemaNamingConvention)
  case ConfigCaseConverter(from: SchemaNamingConvention, to: SchemaNamingConvention)

final case class SchemaAnnotations[A](
  typeId: Option[zio.schema.TypeId],
  entityNameAnnotation: Option[SchemaAnnotation.EntityName],
  default: Option[SchemaAnnotation.Default[A]],
  description: Option[SchemaAnnotation.Description],
  validate: Option[SchemaAnnotation.Validate[A]],
  sumTypeSerDeStrategy: Option[SchemaAnnotation.SumTypeSerDeStrategy[A]],
  jsonCaseConverter: Option[SchemaAnnotation.JsonCaseConverter],
  configCaseConverter: Option[SchemaAnnotation.ConfigCaseConverter]
):
  def transformJsonLabel(label: String): String =
    jsonCaseConverter match
      case Some(converter) => converter.to.fromTokens(converter.from.toTokens(label))
      case None => label

  def transformConfigLabel(label: String): String =
    configCaseConverter match
      case Some(converter) => converter.to.fromTokens(converter.from.toTokens(label))
      case None => label

  def entityName: Option[String] =
    entityNameAnnotation match
      case Some(annotation) => Some(annotation.name)
      case None =>
        typeId match
          case Some(tId) =>
            tId match
              case TypeId.Structural => None
              case TypeId.Nominal(_, _, typeName) => Some(typeName)
          case None => None

  def fullyQualifiedName: Option[String] =
    typeId match
      case Some(tId) =>
        tId match
          case TypeId.Structural => None
          case nominal: TypeId.Nominal => Some(nominal.fullyQualified)
      case None => None
end SchemaAnnotations

object SchemaAnnotations:
  def withTypeId[A](typeId: Option[TypeId]): SchemaAnnotations[A] =
    SchemaAnnotations[A](
      typeId = typeId,
      entityNameAnnotation = None,
      default = None,
      description = None,
      validate = None,
      sumTypeSerDeStrategy = None,
      jsonCaseConverter = None,
      configCaseConverter = None
    )

  def extractAnnotations[A](typeId: Option[TypeId], annotations: Chunk[Any]): SchemaAnnotations[A] =
    annotations.foldLeft[SchemaAnnotations[A]](withTypeId(typeId)) { case (typed, annotation) =>
      annotation match
        case a: SchemaAnnotation.EntityName => typed.copy(entityNameAnnotation = Some(a))
        case a: SchemaAnnotation.Default[A @unchecked] => typed.copy(default = Some(a))
        case a: SchemaAnnotation.Description => typed.copy(description = Some(a))
        case a: SchemaAnnotation.Validate[A @unchecked] => typed.copy(validate = Some(a))
        case a: SchemaAnnotation.SumTypeSerDeStrategy[A @unchecked] => typed.copy(sumTypeSerDeStrategy = Some(a))
        case a: SchemaAnnotation.JsonCaseConverter => typed.copy(jsonCaseConverter = Some(a))
        case a: SchemaAnnotation.ConfigCaseConverter => typed.copy(configCaseConverter = Some(a))
        case _ => typed
    }

end SchemaAnnotations
