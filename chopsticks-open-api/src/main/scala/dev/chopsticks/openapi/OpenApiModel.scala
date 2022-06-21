package dev.chopsticks.openapi

import cats.data.NonEmptyList
import eu.timepit.refined.types.all.{NonEmptyString, PosInt}
import sttp.tapir.{Schema => TapirSchema}
import zio.json.{JsonDecoder, JsonEncoder}
import zio.schema.{Schema => ZioSchema}

trait OpenApiModel[A] {
  implicit def zioSchema: ZioSchema[A]

  implicit lazy val jsonEncoder: JsonEncoder[A] = zio.schema.codec.JsonCodec.Encoder.schemaEncoder(zioSchema)
  implicit lazy val jsonDecoder: JsonDecoder[A] = zio.schema.codec.JsonCodec.Decoder.schemaDecoder(zioSchema)
  implicit lazy val tapirSchema: TapirSchema[A] = OpenApiZioSchemaToTapirConverter.convert(zioSchema)
}

final case class OpenApiNonEmptyString(value: String) extends AnyVal
object OpenApiNonEmptyString extends OpenApiModel[OpenApiNonEmptyString] {
  import OpenApiZioSchemas._
  implicit val zioSchema: ZioSchema[OpenApiNonEmptyString] =
    ZioSchema[String]
      .validate(Validators.nonEmptyStringValidator)
      .transformWithoutAnnotations[OpenApiNonEmptyString](OpenApiNonEmptyString(_), _.value)
  def from(value: String): Either[String, OpenApiNonEmptyString] = {
    NonEmptyString.from(value).map(_ => OpenApiNonEmptyString(value))
  }
  def unsafeFrom(value: String): OpenApiNonEmptyString = {
    OpenApiNonEmptyString(NonEmptyString.unsafeFrom(value).value)
  }
}

final case class OpenApiPosInt(value: Int) extends AnyVal
object OpenApiPosInt extends OpenApiModel[OpenApiPosInt] {
  import OpenApiZioSchemas._
  implicit val zioSchema: ZioSchema[OpenApiPosInt] =
    ZioSchema[Int]
      .validate(Validators.posIntValidator)
      .transformWithoutAnnotations[OpenApiPosInt](OpenApiPosInt(_), _.value)

  def from(value: Int): Either[String, OpenApiPosInt] = {
    PosInt.from(value).map(_ => OpenApiPosInt(value))
  }
  def unsafeFrom(value: Int): OpenApiPosInt = {
    OpenApiPosInt(PosInt.unsafeFrom(value).value)
  }
}

final case class OpenApiNonEmptyList[A](value: List[A]) extends AnyVal
object OpenApiNonEmptyList {
  def fromListUnsafe[A](value: List[A]): OpenApiNonEmptyList[A] = {
    fromNonEmptyList(NonEmptyList.fromListUnsafe(value))
  }
  def fromList[A](value: List[A]): Option[OpenApiNonEmptyList[A]] = {
    Option.when(value.nonEmpty)(OpenApiNonEmptyList(value))
  }
  def fromNonEmptyList[A](value: NonEmptyList[A]): OpenApiNonEmptyList[A] = {
    OpenApiNonEmptyList(value.toList)
  }
}
