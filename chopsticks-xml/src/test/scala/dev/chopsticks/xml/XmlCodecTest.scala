package dev.chopsticks.xml

import dev.chopsticks.xml.XmlAnnotations.xmlFieldName
import zio.schema.{DeriveSchema, Schema}

final case class XmlTestPerson(
  name: String,
  age: Option[Int],
  nickname: Option[String],
  @xmlFieldName("address")
  addresses: List[XmlTestAddress]
)
object XmlTestPerson extends XmlModel[XmlTestPerson] {
  implicit override lazy val zioSchema: Schema[XmlTestPerson] = DeriveSchema.gen
}

final case class XmlTestAddress(city: String, street: String, zip: Option[String])
object XmlTestAddress extends XmlModel[XmlTestAddress] {
  implicit override lazy val zioSchema: Schema[XmlTestAddress] = DeriveSchema.gen
}
