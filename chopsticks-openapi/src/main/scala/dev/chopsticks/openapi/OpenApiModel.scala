package dev.chopsticks.openapi

import dev.chopsticks.util.config.PureconfigFastCamelCaseNamingConvention
import io.circe.{Decoder, Encoder}
import pureconfig.SnakeCase
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

trait OpenApiNamingConvention {
  def toTokens(value: String): Seq[String]
  def fromTokens(tokens: Seq[String]): String
}
object OpenApiNamingConvention {
  final case object OpenApiCamelCase extends OpenApiNamingConvention {
    override def toTokens(value: String): Seq[String] = PureconfigFastCamelCaseNamingConvention.toTokens(value)
    override def fromTokens(tokens: Seq[String]): String = PureconfigFastCamelCaseNamingConvention.fromTokens(tokens)
  }

  final case object OpenApiSnakeCase extends OpenApiNamingConvention {
    override def toTokens(value: String): Seq[String] = SnakeCase.toTokens(value)
    override def fromTokens(tokens: Seq[String]): String = SnakeCase.fromTokens(tokens)
  }
}
