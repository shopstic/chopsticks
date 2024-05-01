package dev.chopsticks.soap.xsd

import dev.chopsticks.xml.XmlAnnotations
import zio.Chunk

final private[chopsticks] case class XsdAnnotations[A](
  xmlAnnotations: XmlAnnotations[A],
  xsdSchemaName: Option[String] = None
)

object XsdAnnotations {
  final case class xsdSchemaName(name: String) extends scala.annotation.StaticAnnotation

  private[chopsticks] def extractAnnotations[A](annotations: Chunk[Any]): XsdAnnotations[A] = {
    val xmlAnnotations = XmlAnnotations.extractAnnotations[A](annotations)
    annotations.foldLeft(XsdAnnotations[A](xmlAnnotations)) { case (typed, annotation) =>
      annotation match {
        case a: xsdSchemaName => typed.copy(xsdSchemaName = Some(a.name))
        case _ => typed
      }
    }
  }
}
