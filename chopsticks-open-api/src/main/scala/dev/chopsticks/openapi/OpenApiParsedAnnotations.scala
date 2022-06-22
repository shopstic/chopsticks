package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiAnnotations.entityName
import sttp.tapir.Schema.annotations.{description, validate}
import sttp.tapir.Validator
import zio.Chunk

final private[openapi] case class OpenApiParsedAnnotations[A](
  entityName: Option[String] = None,
  description: Option[String] = None,
  validator: Option[Validator[A]] = None
)

object OpenApiParsedAnnotations {
  private[openapi] def extractAnnotations[A](annotations: Chunk[Any]): OpenApiParsedAnnotations[A] = {
    annotations.foldLeft(OpenApiParsedAnnotations[A]()) { case (typed, annotation) =>
      annotation match {
        case a: entityName => typed.copy(entityName = Some(a.name))
        case a: description => typed.copy(description = Some(a.text))
        case a: validate[A @unchecked] => typed.copy(validator = Some(a.v))
        case _ => typed
      }
    }
  }
}
