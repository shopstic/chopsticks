package dev.chopsticks.soap.wsdl

import dev.chopsticks.soap.XsdSchema
import dev.chopsticks.soap.wsdl.WsdlParsingError.{SoapEnvelopeParsing, XmlParsing}
import dev.chopsticks.xml.{XmlDecoder, XmlDecoderError, XmlEncoder}
import zio.Chunk

import scala.collection.immutable.{ListMap, ListSet}
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrefixedAttribute}

sealed trait WsdlParsingError extends Product with Serializable
object WsdlParsingError {
  final case class UnknownAction(received: String, supported: Iterable[String]) extends WsdlParsingError
  final case class XmlParsing(message: String, internalException: Throwable) extends WsdlParsingError
  final case class SoapEnvelopeParsing(message: String) extends WsdlParsingError
  final case class XmlDecoder(errors: List[XmlDecoderError]) extends WsdlParsingError
}

// follows subset of the WSDL 1.1 spec
final case class Wsdl(
  definitions: WsdlDefinitions,
  portTypeName: String,
  bindingName: String,
  operations: ListSet[WsdlOperation]
) {
  private lazy val operationsByName = operations.iterator.map(op => op.name -> op).to(ListMap)

  def addOperation(operation: WsdlOperation): Wsdl = {
    copy(operations = this.operations + operation)
  }

  def parseBodyPart1(
    soapAction: String,
    body: String
  ): Either[WsdlParsingError, (WsdlOperation, Any)] = {
    operationsByName.get(soapAction) match {
      case None => Left(WsdlParsingError.UnknownAction(soapAction, operationsByName.keys))
      case Some(operation) =>
        Try(xml.XML.loadString(body)) match {
          case Failure(e) => Left(XmlParsing(s"Provided XML is not valid.", e))
          case Success(xml) =>
            xml.child.collectFirst { case elem: Elem if elem.label == "Body" => elem } match {
              case None => Left(SoapEnvelopeParsing("Body element not found in the provided XML."))
              case Some(bodyElem) =>
                bodyElem.child.collectFirst { case elem: Elem if elem.label == operation.name => elem } match {
                  case None =>
                    Left(SoapEnvelopeParsing(s"${operation.name} element not found within the soapenv:Body."))
                  case Some(operationElem) =>
                    operation.input.parts.headOption match {
                      case None => Left(SoapEnvelopeParsing(s"No input parts found for operation ${operation.name}."))
                      case Some(wsdlPart) =>
                        operationElem.child.collectFirst { case e: Elem if e.label == wsdlPart.name => e } match {
                          case None =>
                            Left(SoapEnvelopeParsing(
                              s"${wsdlPart.name} not found for operation ${operation.name} in the received XML."
                            ))
                          case Some(partElem) =>
                            wsdlPart.xmlDecoder.parse(partElem.child) match {
                              case Left(errors) => Left(WsdlParsingError.XmlDecoder(errors))
                              case Right(value) => Right((operation, value))
                            }
                        }
                    }
                }
            }
        }
    }
  }

  def serializeResponsePart1[A: XsdSchema](operation: WsdlOperation, response: A): Elem = {
    if (operation.output.parts.size != 1) {
      throw new RuntimeException(
        s"Only single part output is supported. Got ${operation.output.parts.size} parts instead."
      )
    }
    if (operation.output.parts.head.xsdSchema != implicitly[XsdSchema[A]]) {
      throw new RuntimeException("XsdSchema[A] must match the schema of the part.")
    }
    val part = operation.output.parts.head.asInstanceOf[WsdlMessagePart[A]]
    Elem(
      "soapenv",
      "Envelope",
      new PrefixedAttribute("xmlns", "soapenv", "http://schemas.xmlsoap.org/soap/envelope/", scala.xml.Null),
      scala.xml.TopScope,
      minimizeEmpty = true,
      Elem(
        "soapenv",
        "Header",
        scala.xml.Null,
        scala.xml.TopScope,
        minimizeEmpty = true
      ),
      Elem(
        "soapenv",
        "Body",
        scala.xml.Null,
        scala.xml.TopScope,
        minimizeEmpty = true,
        Elem(
          definitions.custom.key,
          part.xsdSchema.name,
          new PrefixedAttribute(
            "xmlns",
            definitions.custom.key,
            definitions.targetNamespace,
            new PrefixedAttribute("soapenv", "encodingStyle", "http://schemas.xmlsoap.org/soap/encoding/", xml.Null)
          ),
          scala.xml.TopScope,
          minimizeEmpty = true,
          Elem(
            null,
            part.name,
            xml.Null,
            xml.TopScope,
            minimizeEmpty = true,
            part.xmlEncoder.encode(response): _*
          )
        )
      )
    )
  }
}

object Wsdl {
  def withDefinitions(
    targetNamespace: String,
    portTypeName: String,
    bindingName: String,
    definition: WsdlDefinition
  ): Wsdl = {
    Wsdl(
      definitions = WsdlDefinitions(
        targetNamespace = targetNamespace,
        custom = definition
      ),
      portTypeName = portTypeName,
      bindingName = bindingName,
      operations = ListSet.empty
    )
  }
}

final case class WsdlDefinitions(
  targetNamespace: String,
  xmlns: WsdlDefinitionAddress = WsdlDefinition.wsdl.address,
  wsdl: WsdlDefinitionAddress = WsdlDefinition.wsdl.address,
  wsdlsoap: WsdlDefinitionAddress = WsdlDefinition.wsdlsoap.address,
  xsd: WsdlDefinitionAddress = WsdlDefinition.xsd.address,
  custom: WsdlDefinition
) {
  lazy val defs = Chunk[WsdlDefinition](
    WsdlDefinition(WsdlDefinition.wsdl.key, wsdl),
    WsdlDefinition(WsdlDefinition.wsdlsoap.key, wsdlsoap),
    WsdlDefinition(WsdlDefinition.xsd.key, xsd),
    custom
  )
}

final case class WsdlDefinition(key: String, address: WsdlDefinitionAddress)
object WsdlDefinition {
  val wsdl = WsdlDefinition("wsdl", WsdlDefinitionAddress("http://schemas.xmlsoap.org/wsdl/"))
  val wsdlsoap = WsdlDefinition("wsdlsoap", WsdlDefinitionAddress("http://schemas.xmlsoap.org/wsdl/soap/"))
  val xsd = WsdlDefinition("xsd", WsdlDefinitionAddress("http://www.w3.org/2001/XMLSchema"))
}

final case class WsdlDefinitionAddress(value: String) extends AnyVal

final case class WsdlMessage(name: String, parts: ListSet[WsdlMessagePart[_]])

final case class WsdlMessagePart[A](name: String)(implicit
  val xmlEncoder: XmlEncoder[A],
  val xmlDecoder: XmlDecoder[A],
  val xsdSchema: XsdSchema[A]
)

final case class WsdlOperation(name: String, input: WsdlMessage, output: WsdlMessage)
