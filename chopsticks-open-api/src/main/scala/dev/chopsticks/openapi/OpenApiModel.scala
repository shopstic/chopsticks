package dev.chopsticks.openapi

import io.circe.{Decoder, Encoder}
import sttp.tapir.{Schema => TapirSchema}
import zio.schema.{Schema => ZioSchema}

import scala.annotation.nowarn

trait OpenApiModel[A] {
  implicit def zioSchema: ZioSchema[A]

  implicit lazy val jsonEncoder: Encoder[A] = OpenApiZioSchemaCirceConverter.Encoder.convert(zioSchema)
  implicit lazy val jsonDecoder: Decoder[A] = OpenApiZioSchemaCirceConverter.Decoder.convert(zioSchema)
  implicit lazy val tapirSchema: TapirSchema[A] = OpenApiZioSchemaToTapirConverter.convert(zioSchema)
}

sealed trait OpenApiSumTypeSerDeStrategy[A]
object OpenApiSumTypeSerDeStrategy {
  final case class Discriminator[A](discriminator: OpenApiDiscriminator[A]) extends OpenApiSumTypeSerDeStrategy[A]
//  final case object Nested extends OpenApiSumTypeSerDeStrategy
}

trait OpenApiDiscriminator[A] {
  @nowarn("cat=unused-privates")
  private type DiscriminatorValue = String
  @nowarn("cat=unused-privates")
  private type SubTypeName = String

  def discriminatorFieldName: String
  def mapping: Map[DiscriminatorValue, SubTypeName]
  def discriminatorValue(obj: A): DiscriminatorValue
}
