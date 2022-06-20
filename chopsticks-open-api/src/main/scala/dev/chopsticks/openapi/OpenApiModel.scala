package dev.chopsticks.openapi

import sttp.tapir.{Schema => TapirSchema}
import zio.json.{JsonDecoder, JsonEncoder}
import zio.schema.{Schema => ZioSchema}

trait OpenApiModel[A] {
  implicit def zioSchema: ZioSchema[A]

  implicit lazy val jsonEncoder: JsonEncoder[A] = zio.schema.codec.JsonCodec.Encoder.schemaEncoder(zioSchema)
  implicit lazy val jsonDecoder: JsonDecoder[A] = zio.schema.codec.JsonCodec.Decoder.schemaDecoder(zioSchema)
  implicit lazy val tapirSchema: TapirSchema[A] = OpenApiZioSchemaToTapirConverter.convert(zioSchema)
}
