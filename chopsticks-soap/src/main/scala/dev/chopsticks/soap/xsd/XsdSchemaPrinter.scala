package dev.chopsticks.soap.xsd

import dev.chopsticks.soap.XsdSchema
import dev.chopsticks.xml.XmlPrettyPrinter

import scala.xml.{Elem, TopScope, UnprefixedAttribute}

object XsdSchemaPrinter {

  def printSchema[A](targetNamespaceName: String, namespaceName: String, schema: XsdSchema[A]): String = {
    val types = schema.xsdTypes(namespaceName)
    val sortedKeys = types.keys.toSeq.sortBy { case (t, n) => (t.orderPriority, n) }
    val elements = sortedKeys.map(k => types(k))
    val xmlDoc = Elem(
      "xsd",
      "schema",
      new UnprefixedAttribute("targetNamespace", targetNamespaceName, scala.xml.Null),
      TopScope,
      minimizeEmpty = true,
      elements: _*
    )
    renderAsXml(xmlDoc)
  }

  private def renderAsXml(value: Elem): String = {
    new XmlPrettyPrinter(120, 2, minimizeEmpty = true).format(value)
  }

}
