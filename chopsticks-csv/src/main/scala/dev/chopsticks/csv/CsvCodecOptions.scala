package dev.chopsticks.csv

import dev.chopsticks.util.config.PureconfigFastCamelCaseNamingConvention
import pureconfig.SnakeCase

final case class CsvCodecOptions(
  maxSeqSize: Int,
  nestedFieldLabel: (Option[String], String) => String,
  nestedArrayFieldLabel: (Option[String], Int) => String
)

object CsvCodecOptions {
  val default = {
    val toSnakeCase: String => String =
      value => SnakeCase.fromTokens(PureconfigFastCamelCaseNamingConvention.toTokens(value))
    CsvCodecOptions(
      maxSeqSize = 10,
      nestedFieldLabel = {
        case (None, fieldName) => toSnakeCase(fieldName)
        case (Some(prefix), fieldName) => prefix + "_" + toSnakeCase(fieldName)
      },
      nestedArrayFieldLabel = {
        case (Some(prefix), index) => prefix + "_" + (index + 1).toString
        case (None, index) => (index + 1).toString
      }
    )
  }
}
