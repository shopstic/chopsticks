package dev.chopsticks.soap.wsdl

import dev.chopsticks.soap.XsdType
import dev.chopsticks.xml.XmlPrettyPrinter

import scala.xml.{Elem, MetaData, Node, NodeSeq, PrefixedAttribute, TopScope, UnprefixedAttribute}

object WsdlSchemaPrinter {

  def printSchema(wsdl: Wsdl): String = {
    val xmlDoc = Elem(
      "wsdl",
      "definitions",
      new UnprefixedAttribute(
        "targetNamespace",
        wsdl.definitions.targetNamespace,
        new UnprefixedAttribute(
          "xmlns",
          wsdl.definitions.xmlns.value,
          wsdl.definitions.defs.foldRight(scala.xml.Null: MetaData) { case (definition, nextAttr) =>
            new PrefixedAttribute("xmlns", definition.key, definition.address.value, nextAttr)
          }
        )
      ),
      TopScope,
      minimizeEmpty = true,
      NodeSeq
        .newBuilder
        .addOne(wsdlTypes(wsdl))
        .addAll(wsdlMessages(wsdl))
        .addOne(portType(wsdl))
        .addOne(bindings(wsdl))
        .result(): _*
    )
    s"""<?xml version="1.0" encoding="UTF-8"?>
       |${renderAsXml(xmlDoc)}""".stripMargin
  }

  private def wsdlTypes(wsdl: Wsdl): Elem = {
    val messageTypes = wsdl.operations.iterator
      .flatMap(op => op.input.parts.iterator.concat(op.output.parts.iterator))
      .map(_.xsdSchema)
    val types =
      messageTypes.foldLeft(scala.collection.mutable.LinkedHashMap.empty[(XsdType, String), Elem]) { (acc, schema) =>
        schema.collectXsdTypes(wsdl.definitions.custom.key, acc)
      }
    val sortedKeys = types.keys.toSeq.sortBy { case (t, n) => (t.orderPriority, n) }
    val elements = sortedKeys.map(k => types(k))
    Elem(
      "wsdl",
      "types",
      xml.Null,
      TopScope,
      minimizeEmpty = true,
      Elem(
        "xsd",
        "schema",
        new UnprefixedAttribute("targetNamespace", wsdl.definitions.targetNamespace, scala.xml.Null),
        TopScope,
        minimizeEmpty = true,
        elements: _*
      )
    )
  }

  private def wsdlMessages(wsdl: Wsdl): NodeSeq = {
    wsdl
      .operations
      .iterator
      .flatMap { op =>
        Iterator(op.input, op.output)
      }
      .map { message =>
        Elem(
          "wsdl",
          "message",
          new UnprefixedAttribute("name", message.name, scala.xml.Null),
          TopScope,
          minimizeEmpty = true,
          message.parts.toList.map { part =>
            Elem(
              "wsdl",
              "part",
              new UnprefixedAttribute(
                "name",
                part.name,
                new UnprefixedAttribute(
                  "type",
                  part.xsdSchema.namespacedName(wsdl.definitions.custom.key),
                  scala.xml.Null
                )
              ),
              TopScope,
              minimizeEmpty = true
            )
          }: _*
        )
      }
      .foldLeft(NodeSeq.newBuilder)((acc, elem) => acc += elem)
      .result()
  }

  private def portType(wsdl: Wsdl): Elem = {
    val operations = wsdl.operations.iterator
      .map { op =>
        Elem(
          "wsdl",
          "operation",
          new UnprefixedAttribute("name", op.name, scala.xml.Null),
          TopScope,
          minimizeEmpty = true,
          Elem(
            "wsdl",
            "input",
            new UnprefixedAttribute("message", s"${wsdl.definitions.custom.key}:${op.input.name}", scala.xml.Null),
            TopScope,
            minimizeEmpty = true
          ),
          Elem(
            "wsdl",
            "output",
            new UnprefixedAttribute("message", s"${wsdl.definitions.custom.key}:${op.output.name}", scala.xml.Null),
            TopScope,
            minimizeEmpty = true
          )
        )
      }
      .toVector
    Elem(
      "wsdl",
      "portType",
      new UnprefixedAttribute("name", wsdl.portTypeName, scala.xml.Null),
      TopScope,
      minimizeEmpty = true,
      operations: _*
    )
  }

  private def bindings(wsdl: Wsdl) = {
    Elem(
      "wsdl",
      "binding",
      new UnprefixedAttribute(
        "name",
        wsdl.bindingName,
        new UnprefixedAttribute("type", s"${wsdl.definitions.custom.key}:${wsdl.portTypeName}", scala.xml.Null)
      ),
      TopScope,
      minimizeEmpty = true,
      Iterator
        .single {
          Elem(
            "wsdlsoap",
            "binding",
            new UnprefixedAttribute(
              "style",
              "rpc",
              new UnprefixedAttribute("transport", "http://schemas.xmlsoap.org/soap/http", scala.xml.Null)
            ),
            TopScope,
            minimizeEmpty = true
          )
        }
        .concat {
          wsdl.operations.toVector.map { op =>
            bindingOperation(wsdl, op)
          }
        }
        .toVector: _*
    )
  }

  private def bindingOperation(wsdl: Wsdl, operation: WsdlOperation): Elem = {
    Elem(
      "wsdl",
      "operation",
      new UnprefixedAttribute("name", operation.name, scala.xml.Null),
      TopScope,
      minimizeEmpty = true,
      Elem(
        "wsdlsoap",
        "operation",
        new UnprefixedAttribute("soapAction", s"${operation.name}", scala.xml.Null),
        TopScope,
        minimizeEmpty = true
      ),
      Elem(
        "wsdl",
        "input",
        scala.xml.Null,
        TopScope,
        minimizeEmpty = true,
        wsdlSoapBody(wsdl)
      ),
      Elem(
        "wsdl",
        "output",
        scala.xml.Null,
        TopScope,
        minimizeEmpty = true,
        wsdlSoapBody(wsdl)
      )
    )
  }

  private def wsdlSoapBody(wsdl: Wsdl): Elem = {
    Elem(
      "wsdlsoap",
      "body",
      new UnprefixedAttribute(
        "encodingStyle",
        "http://schemas.xmlsoap.org/soap/encoding/",
        new UnprefixedAttribute(
          "namespace",
          wsdl.definitions.targetNamespace,
          new UnprefixedAttribute("use", "encoded", scala.xml.Null)
        )
      ),
      TopScope,
      minimizeEmpty = true
    )
  }

  private def renderAsXml(value: Node): String = {
    new XmlPrettyPrinter(120, 2, minimizeEmpty = true).format(value)
  }

}
