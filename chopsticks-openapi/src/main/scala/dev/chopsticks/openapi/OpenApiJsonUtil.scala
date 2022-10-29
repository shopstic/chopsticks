package dev.chopsticks.openapi

import io.circe.{CursorOp, Json, Printer}

import scala.util.control.NoStackTrace

object OpenApiJsonUtil {
  val jsonPrinterNoSpaces = Printer(
    dropNullValues = true,
    indent = ""
  )
}

final case class OpenApiJsonParsingException(message: String, underlying: Throwable)
    extends RuntimeException
    with NoStackTrace

final case class OpenApiJsonDecodeException(errors: List[OpenApiJsonError], underlying: Throwable)
    extends RuntimeException
    with NoStackTrace

final case class OpenApiJsonError(original: Json, history: List[CursorOp], message: String)
    extends RuntimeException
    with NoStackTrace {
  lazy val jsonPath: String = CursorOp.opsToPath(history)
}
