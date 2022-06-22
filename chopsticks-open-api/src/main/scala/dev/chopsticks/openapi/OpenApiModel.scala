package dev.chopsticks.openapi

import io.circe.{Decoder, Encoder}
import sttp.tapir.{Schema => TapirSchema}
import zio.schema.{Schema => ZioSchema}

trait OpenApiModel[A] {
  implicit def zioSchema: ZioSchema[A]

  implicit lazy val jsonEncoder: Encoder[A] = OpenApiZioSchemaCirceConverter.Encoder.convert(zioSchema)
  implicit lazy val jsonDecoder: Decoder[A] = OpenApiZioSchemaCirceConverter.Decoder.convert(zioSchema)
  implicit lazy val tapirSchema: TapirSchema[A] = OpenApiZioSchemaToTapirConverter.convert(zioSchema)
}
