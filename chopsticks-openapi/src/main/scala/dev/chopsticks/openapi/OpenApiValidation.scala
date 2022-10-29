package dev.chopsticks.openapi

import sttp.tapir.{ValidationError, Validator}

object OpenApiValidation {
  def errorMessage(validationError: ValidationError[_]): String = {
    validationError match {
      case primitive: ValidationError.Primitive[_] =>
        primitive.validator match {
          case v: Validator.Min[_] =>
            s"Value must be greater${if (v.exclusive) "" else " or equal"} than ${v.value}. Received: ${validationError.invalidValue}."
          case v: Validator.Max[_] =>
            s"Value must be smaller${if (v.exclusive) "" else " or equal"} than ${v.value}. Received: ${validationError.invalidValue}."
          case pattern: Validator.Pattern[_] =>
            s"Value must match the pattern: ${pattern.value}. Received: '${validationError.invalidValue}'."
          case v: Validator.MinLength[_] =>
            val value = validationError.invalidValue.toString
            if (value.isEmpty) {
              s"Length of the value must be greater or equal than ${v.value}. Received empty value."
            }
            else {
              s"Length of the value must be greater or equal than ${v.value}. Received: '${validationError.invalidValue}'."
            }
          case v: Validator.MaxLength[_] =>
            val value = validationError.invalidValue.toString
            val truncated = value.take(v.value + 1)
            val charsLeft = value.length - truncated.length
            val formatted =
              if (charsLeft <= 0) value
              else s"""$truncated[truncated to ${truncated.length}](+$charsLeft more)""""
            s"Length of the value must be smaller or equal than ${v.value}. Received: '$formatted'."
          case v: Validator.MinSize[_, _] =>
            s"Size of the provided array must be greater or equal than ${v.value}. Received array of size ${validationError.invalidValue.asInstanceOf[Iterable[_]].size}."
          case v: Validator.MaxSize[_, _] =>
            s"Size of the provided array must be smaller or equal than ${v.value}. Received array of size ${validationError.invalidValue.asInstanceOf[Iterable[_]].size}."
          case e: Validator.Enumeration[_] =>
            s"Value must be one of: ${e.possibleValues.mkString(", ")}. Received: '${validationError.invalidValue}'."
        }
      case custom: ValidationError.Custom[_] =>
        custom.message
    }
  }
}
