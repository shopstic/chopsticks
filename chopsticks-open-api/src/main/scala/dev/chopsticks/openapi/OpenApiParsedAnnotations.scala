package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiAnnotations._
import sttp.tapir.Validator
import zio.Chunk

final private[openapi] case class OpenApiParsedAnnotations[A](
  entityName: Option[String] = None,
  description: Option[String] = None,
  validator: Option[Validator[A]] = None,
  default: Option[(A, Option[Any])] = None
)

object OpenApiParsedAnnotations {
  private[openapi] def extractAnnotations[A](annotations: Chunk[Any]): OpenApiParsedAnnotations[A] = {
    annotations.foldLeft(OpenApiParsedAnnotations[A]()) { case (typed, annotation) =>
      annotation match {
        case a: entityName => typed.copy(entityName = Some(a.name))
        case a: description => typed.copy(description = Some(a.text))
        case a: validate[A @unchecked] => typed.copy(validator = Some(a.v))
        case a: default[A @unchecked] => typed.copy(default = Some((a.value, a.encodedValue)))
        case _ => typed
      }
    }
  }
}
