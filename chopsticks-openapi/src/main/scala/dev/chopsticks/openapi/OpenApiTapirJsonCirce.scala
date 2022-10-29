package dev.chopsticks.openapi

import cats.data.{Validated, ValidatedNel}
import io.circe._
import io.circe.syntax._
import sttp.tapir._
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.DecodeResult.{Error, Value}
import sttp.tapir.Schema.SName
import sttp.tapir.SchemaType._

// adjusted from Tapir's repository
trait OpenApiTapirJsonCirce {
  private val DefaultCirceMessage = "Attempt to decode value on failed cursor"

  // Json is a coproduct with unknown implementations
  implicit val schemaForCirceJson: Schema[Json] =
    Schema(
      SCoproduct(Nil, None)(_ => None),
      None
    )

  implicit val schemaForCirceJsonObject: Schema[JsonObject] = Schema(SProduct(Nil), Some(SName("io.circe.JsonObject")))

  def jsonBody[T: Encoder: Decoder: Schema: OpenApiErrorCollectingMode]: EndpointIO.Body[String, T] =
    anyFromUtf8StringBody(circeCodec[T])

  implicit def circeCodec[T: Encoder: Decoder: Schema: OpenApiErrorCollectingMode]: JsonCodec[T] = {
    // todo override schema and always use Validator.pass? (Validation happens in circe)
    sttp.tapir.Codec.json[T] { s =>
      io.circe.parser.parse(s) match {
        case Left(error) =>
          Error(
            original = s,
            error = OpenApiJsonParsingException(error.message, error)
          )
        case Right(json) =>
          val validated =
            if (OpenApiErrorCollectingMode[T].accumulateErrors) decodeAccumulating[T](json)
            else decode[T](json)
          validated match {
            case Validated.Valid(v) => Value(v)
            case Validated.Invalid(circeFailures) =>
              val jsonErrors = circeFailures.map { failure: DecodingFailure =>
                OpenApiJsonError(json, failure.history, reformatError(failure.message))
              }
              Error(
                original = s,
                error = OpenApiJsonDecodeException(
                  errors = jsonErrors.toList,
                  underlying = Errors(circeFailures)
                )
              )
          }
      }
    } { t => jsonPrinter.print(t.asJson) }
  }

  def jsonPrinter: Printer = OpenApiJsonUtil.jsonPrinterNoSpaces

  private def decode[A](json: Json)(implicit decoder: Decoder[A]): ValidatedNel[DecodingFailure, A] = {
    decoder.decodeJson(json) match {
      case Right(v) => Validated.Valid(v)
      case Left(e) => Validated.invalidNel(e)
    }
  }

  private def decodeAccumulating[A](json: Json)(implicit decoder: Decoder[A]): ValidatedNel[DecodingFailure, A] = {
    decoder.decodeAccumulating(json.hcursor)
  }

  private def reformatError(value: String): String = {
    value match {
      case DefaultCirceMessage => "Missing or invalid value"
      case other => other
    }
  }
}

object OpenApiTapirJsonCirce extends OpenApiTapirJsonCirce

trait OpenApiErrorCollectingMode[A] {
  def accumulateErrors: Boolean
}

object OpenApiErrorCollectingMode extends OpenApiErrorCollectingModeLowPriorityImplicits {
  def apply[A: OpenApiErrorCollectingMode] = implicitly[OpenApiErrorCollectingMode[A]]

  def create[A](accumulate: Boolean): OpenApiErrorCollectingMode[A] = new OpenApiErrorCollectingMode[A] {
    override def accumulateErrors: Boolean = accumulate
  }
}

trait OpenApiErrorCollectingModeLowPriorityImplicits {
  implicit def defaultCollectingMode[A]: OpenApiErrorCollectingMode[A] = new OpenApiErrorCollectingMode[A] {
    override def accumulateErrors = true
  }
}
