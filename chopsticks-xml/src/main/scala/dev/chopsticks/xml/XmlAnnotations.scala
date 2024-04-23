package dev.chopsticks.xml

import dev.chopsticks.openapi.OpenApiParsedAnnotations
import zio.Chunk

final private[chopsticks] case class XmlAnnotations[A](
  openApiAnnotations: OpenApiParsedAnnotations[A],
  xmlFieldName: Option[String] = None,
  xmlSeqNodeName: Option[String] = None
)

object XmlAnnotations {
  final case class xmlFieldName(name: String) extends scala.annotation.StaticAnnotation
  final case class xmlSeqNodeName(name: String) extends scala.annotation.StaticAnnotation

  private[chopsticks] def extractAnnotations[A](annotations: Chunk[Any]): XmlAnnotations[A] = {
    val openApiAnnotations = OpenApiParsedAnnotations.extractAnnotations[A](annotations)
    annotations.foldLeft(XmlAnnotations[A](openApiAnnotations)) { case (typed, annotation) =>
      annotation match {
        case a: xmlFieldName => typed.copy(xmlFieldName = Some(a.name))
        case a: xmlSeqNodeName => typed.copy(xmlSeqNodeName = Some(a.name))
        case _ => typed
      }
    }
  }
}
