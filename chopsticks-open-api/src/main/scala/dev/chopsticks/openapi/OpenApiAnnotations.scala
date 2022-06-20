package dev.chopsticks.openapi

import scala.annotation.StaticAnnotation

object OpenApiAnnotations {
  final case class entityName(name: String) extends StaticAnnotation
}
