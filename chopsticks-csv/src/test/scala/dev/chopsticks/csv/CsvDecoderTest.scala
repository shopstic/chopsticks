package dev.chopsticks.csv

import dev.chopsticks.csv.CsvDecoderTest.{
  CsvDecoderTestAddress,
  CsvDecoderTestAddressList,
  CsvDecoderTestName,
  CsvDecoderTestPerson
}
import dev.chopsticks.openapi.OpenApiAnnotations.entityName
import dev.chopsticks.openapi.OpenApiModel
import dev.chopsticks.openapi.OpenApiZioSchemas._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.matchers.should.Matchers
import org.scalatest.Assertions
import org.scalatest.wordspec.AnyWordSpecLike
import sttp.tapir.Validator
import zio.schema.{DeriveSchema, Schema}

object CsvDecoderTest {
  @entityName("CsvDecoderTestPerson")
  final case class CsvDecoderTestPerson(age: PosInt, name: CsvDecoderTestName, address: CsvDecoderTestAddressList)
  object CsvDecoderTestPerson extends OpenApiModel[CsvDecoderTestPerson] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestPerson] = DeriveSchema.gen
  }

  final case class CsvDecoderTestName(value: String) extends AnyVal
  object CsvDecoderTestName extends OpenApiModel[CsvDecoderTestName] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestName] =
      Schema[String]
        .validate(Validator.all(Validator.minLength(1), Validator.maxLength(100)))
        .mapBoth(CsvDecoderTestName(_), _.value)
  }

  final case class CsvDecoderTestAddressList(value: List[CsvDecoderTestAddress]) extends AnyVal
  object CsvDecoderTestAddressList extends OpenApiModel[CsvDecoderTestAddressList] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestAddressList] =
      Schema[List[CsvDecoderTestAddress]]
        .validate(Validator.minSize(1))
        .mapBoth(CsvDecoderTestAddressList(_), _.value)
  }

  @entityName("CsvDecoderTestAddress")
  final case class CsvDecoderTestAddress(street: NonEmptyString, city: NonEmptyString)
  object CsvDecoderTestAddress extends OpenApiModel[CsvDecoderTestAddress] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestAddress] = DeriveSchema.gen
  }
}

final class CsvDecoderTest extends AnyWordSpecLike with Assertions with Matchers {
  import eu.timepit.refined.auto._

  "CsvDecoder" should {
    val decoder = CsvDecoder.derive[CsvDecoderTestPerson]()
    "decode valid row" in {
      val row = Map(
        "age" -> "30",
        "name" -> "John",
        "address_2_city" -> "LA",
        "address_2_street" -> "2nd",
        "address_1_city" -> "NY",
        "address_1_street" -> "1st"
      )
      val expected = CsvDecoderTestPerson(
        age = 30,
        name = CsvDecoderTestName("John"),
        address = CsvDecoderTestAddressList(
          List(
            CsvDecoderTestAddress(
              city = "NY",
              street = "1st"
            ),
            CsvDecoderTestAddress(
              city = "LA",
              street = "2nd"
            )
          )
        )
      )
      val result = decoder.parse(row, None)
      assert(result == Right(expected))
    }

    "decode valid row and skip empty trailing columns for list" in {
      val row = Map(
        "age" -> "30",
        "name" -> "John",
        "address_2_city" -> "",
        "address_2_street" -> "",
        "address_1_city" -> "NY",
        "address_1_street" -> "1st"
      )
      val expected = CsvDecoderTestPerson(
        age = 30,
        name = CsvDecoderTestName("John"),
        address = CsvDecoderTestAddressList(
          List(
            CsvDecoderTestAddress(
              city = "NY",
              street = "1st"
            )
          )
        )
      )
      val result = decoder.parse(row, None)
      assert(result == Right(expected))
    }

    "fail decoding if a type of a column is incorrect" in {
      val row = Map(
        "age" -> "Incorrect",
        "name" -> "John",
        "address_2_city" -> "LA",
        "address_2_street" -> "2nd",
        "address_1_city" -> "NY",
        "address_1_street" -> "1st"
      )
      val expected = Left(List(CsvDecoderError("Cannot parse number.", Some("age"))))
      val result = decoder.parse(row, None)
      assert(result == expected)
    }

    "fail decoding if value of a column fails validation" in {
      val row = Map(
        "age" -> "-190",
        "name" -> "John",
        "address_2_city" -> "LA",
        "address_2_street" -> "2nd",
        "address_1_city" -> "NY",
        "address_1_street" -> "1st"
      )
      val expected = Left(List(CsvDecoderError("Value must be greater or equal than 1. Received: -190.", Some("age"))))
      val result = decoder.parse(row, None)
      assert(result == expected)
    }

    "accumulate errors" in {
      val row = Map(
        "age" -> "-190",
        "name" -> "",
        "address_2_city" -> "LA",
        "address_2_street" -> "2nd",
        "address_1_city" -> "",
        "address_1_street" -> "1st"
      )
      val expected = Left(
        List(
          CsvDecoderError("Value must be greater or equal than 1. Received: -190.", Some("age")),
          CsvDecoderError("Length of the value must be greater or equal than 1. Received empty value.", Some("name")),
          CsvDecoderError(
            "Length of the value must be greater or equal than 1. Received empty value.",
            Some("address_1_city")
          )
        )
      )
      val result = decoder.parse(row, None)
      assert(result == expected)
    }

  }
}
