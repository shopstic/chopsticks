package dev.chopsticks.csv

final case class CsvDecoderError(message: String, columnName: Option[String]) {
  def format: String = {
    val trimmed = message.trim
    val withDot = if (trimmed.endsWith(".")) trimmed else trimmed + "."
    withDot + (if (columnName.isDefined) s" Column(s): ${columnName.get}" else "")
  }
}
object CsvDecoderError {
  def columnNotExists(columnName: String): CsvDecoderError = {
    CsvDecoderError(s"Required column does not exist.", columnName = Some(columnName))
  }
  def notAllRequiredColumnsExist(columnName: Option[String]) = {
    CsvDecoderError("Not all required columns contain defined values.", columnName)
  }
  def rowContainsNonExistingTags(nonExistingTags: Iterable[String]) = {
    CsvDecoderError(
      s"The following tags were provided, but are not defined for the customer: ${nonExistingTags.mkString(", ")}",
      None
    )
  }
}
