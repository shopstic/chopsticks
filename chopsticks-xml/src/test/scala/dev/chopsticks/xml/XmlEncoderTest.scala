package dev.chopsticks.xml

import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

import scala.xml.{Elem, Node, NodeBuffer, Null, Text, TopScope, Utility}

final class XmlEncoderTest extends AnyWordSpecLike with Assertions with Matchers {

  "XmlEncoder" should {
    "encode a simple case class" in {
      val address = XmlTestAddress("NY", "1st street", None)
      val encoded = renderAsXml(address)
      val expected =
        <city>NY</city>
        <street>1st street</street>

      assert(encoded == renderAsXml(expected))
    }

    "encode a nested case class" in {
      val person = XmlTestPerson(
        name = "John",
        age = Some(30),
        nickname = None,
        addresses = List(
          XmlTestAddress("NY", "1st street", None),
          XmlTestAddress("LA", "2nd street", Some("12345"))
        )
      )
      val encoded = renderAsXml(person)
      val expected = {
        <name>John</name>
        <age>30</age>
        <addressData>
          <address>
            <city>NY</city>
            <street>1st street</street>
          </address>
          <address>
            <city>LA</city>
            <street>2nd street</street>
            <zip>12345</zip>
          </address>
        </addressData>
      }
      val expectedEncoded = renderAsXml(expected)
      assert(expectedEncoded == encoded)
    }
  }

  private def renderAsXml[A: XmlEncoder](value: A): String = {
    val nodes = implicitly[XmlEncoder[A]].encode(value)
    val wrapped = Elem(null, "root", Null, TopScope, minimizeEmpty = true, nodes: _*)
    val cleanedXml = trimTextNodes(wrapped)
    Utility.serialize(cleanedXml).result()
  }

  private def renderAsXml(value: NodeBuffer): String = {
    val wrapped = Elem(null, "root", Null, TopScope, minimizeEmpty = true, value: _*)
    val cleanedXml = trimTextNodes(wrapped)
    Utility.serialize(cleanedXml).result()
  }

  // Helper function to recursively trim text nodes
  private def trimTextNodes(node: Node): Node = node match {
    case Elem(prefix, label, attribs, scope, child @ _*) =>
      Elem(prefix, label, attribs, scope, minimizeEmpty = true, child.map(trimTextNodes): _*)
    case Text(data) =>
      Text(data.trim)
    case other => other
  }

}
