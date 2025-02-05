package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiAnnotations._
import sttp.tapir.Validator
import zio.Chunk

final private[chopsticks] case class OpenApiParsedAnnotations[A](
  entityName: Option[String] = None,
  description: Option[String] = None,
  validator: Option[Validator[A]] = None,
  default: Option[(A, Option[Any])] = None,
  sumTypeSerDeStrategy: Option[OpenApiSumTypeSerDeStrategy[A]] = None,
  jsonCaseConverter: Option[jsonCaseConverter] = None,
  jsonEncoder: Option[io.circe.Encoder[A]] = None,
  jsonDecoder: Option[io.circe.Decoder[A]] = None,
  tapirSchema: Option[sttp.tapir.Schema[A]] = None
) {
  def transformJsonLabel(label: String): String = {
    jsonCaseConverter match {
      case None => label
      case Some(converter) =>
        converter.to.fromTokens(converter.from.toTokens(label))
    }
  }
}

object OpenApiParsedAnnotations {
  private[chopsticks] def extractAnnotations[A](annotations: Chunk[Any]): OpenApiParsedAnnotations[A] = {
    annotations.foldLeft(OpenApiParsedAnnotations[A]()) { case (typed, annotation) =>
      annotation match {
        case a: entityName => typed.copy(entityName = Some(a.name))
        case a: description => typed.copy(description = Some(a.text))
        case a: validate[A @unchecked] => typed.copy(validator = Some(a.v))
        case a: default[A @unchecked] => typed.copy(default = Some((a.value, a.encodedValue)))
        case a: sumTypeSerDeStrategy[A @unchecked] => typed.copy(sumTypeSerDeStrategy = Some(a.value))
        case a: jsonCaseConverter => typed.copy(jsonCaseConverter = Some(a))
        case a: jsonEncoder[A @unchecked] => typed.copy(jsonEncoder = Some(a.value))
        case a: jsonDecoder[A @unchecked] => typed.copy(jsonDecoder = Some(a.value))
        case a: tapirSchema[A @unchecked] => typed.copy(tapirSchema = Some(a.value))
        case _ => typed
      }
    }
  }
}
