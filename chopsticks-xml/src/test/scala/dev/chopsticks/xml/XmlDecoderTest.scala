package dev.chopsticks.xml

import org.scalatest.matchers.should.Matchers
import org.scalatest.Assertions
import org.scalatest.wordspec.AnyWordSpecLike

final class XmlDecoderTest extends AnyWordSpecLike with Assertions with Matchers {
  "XmlDecoder" should {
    "decode a simple case class" in {
      val xml =
        <city>NY</city>
        <street>1st street</street>
      val decoded = XmlTestAddress.xmlDecoder.parse(xml)
      val expected = XmlTestAddress("NY", "1st street", None)
      assert(decoded == Right(expected))
    }

    "decode a nested case class" in {
      val xml = {
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
      val expected = XmlTestPerson(
        name = "John",
        age = Some(30),
        nickname = None,
        addresses = List(
          XmlTestAddress("NY", "1st street", None),
          XmlTestAddress("LA", "2nd street", Some("12345"))
        )
      )
      val decoded = XmlTestPerson.xmlDecoder.parse(xml)
      assert(decoded == Right(expected))
    }
  }
}
