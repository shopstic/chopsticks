package dev.chopsticks.xml

import zio.schema.Schema

trait XmlModel[A] {
  implicit def zioSchema: Schema[A]
  implicit lazy val xmlEncoder: XmlEncoder[A] = XmlEncoder.derive[A]()(zioSchema)
  implicit lazy val xmlDecoder: XmlDecoder[A] = XmlDecoder.derive[A]()(zioSchema)
}
