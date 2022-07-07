package dev.chopsticks.openapi

import sttp.tapir.Validator

import scala.annotation.StaticAnnotation

object OpenApiAnnotations {
  final case class entityName(name: String) extends StaticAnnotation
  final case class default[A](value: A, encodedValue: Option[Any] = None) extends StaticAnnotation
  final case class description(text: String) extends StaticAnnotation
  final case class validate[T](v: Validator[T]) extends StaticAnnotation
}
