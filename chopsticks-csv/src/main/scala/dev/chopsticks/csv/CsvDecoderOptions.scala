package dev.chopsticks.csv

import dev.chopsticks.util.config.PureconfigFastCamelCaseNamingConvention
import pureconfig.SnakeCase

final case class CsvDecoderOptions(
  maxSeqSize: Int,
  selectNestedField: (Option[String], String) => String,
  selectNestedArrayField: (Option[String], Int) => String
)

object CsvDecoderOptions {
  val default = {
    val toSnakeCase: String => String =
      value => SnakeCase.fromTokens(PureconfigFastCamelCaseNamingConvention.toTokens(value))
    CsvDecoderOptions(
      maxSeqSize = 10,
      selectNestedField = {
        case (None, fieldName) => toSnakeCase(fieldName)
        case (Some(prefix), fieldName) => prefix + "_" + toSnakeCase(fieldName)
      },
      selectNestedArrayField = {
        case (Some(prefix), index) => prefix + "_" + (index + 1).toString
        case (None, index) => (index + 1).toString
      }
    )
  }
}
