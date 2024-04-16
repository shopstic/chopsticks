package dev.chopsticks.csv

import dev.chopsticks.csv.CsvDecoderTest.{
  CsvDecoderTestAddress,
  CsvDecoderTestAddressList,
  CsvDecoderTestItem,
  CsvDecoderTestMessage,
  CsvDecoderTestMultimediaMessage,
  CsvDecoderTestName,
  CsvDecoderTestPerson
}
import dev.chopsticks.openapi.OpenApiAnnotations.entityName
import dev.chopsticks.openapi.{OpenApiDiscriminator, OpenApiModel}
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
        .validated(Validator.all(Validator.minLength(1), Validator.maxLength(100)))
        .mapBoth(CsvDecoderTestName(_), _.value)
  }

  final case class CsvDecoderTestAddressList(value: List[CsvDecoderTestAddress]) extends AnyVal
  object CsvDecoderTestAddressList extends OpenApiModel[CsvDecoderTestAddressList] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestAddressList] =
      Schema[List[CsvDecoderTestAddress]]
        .validated(Validator.minSize(1))
        .mapBoth(CsvDecoderTestAddressList(_), _.value)
  }

  @entityName("CsvDecoderTestAddress")
  final case class CsvDecoderTestAddress(street: NonEmptyString, city: NonEmptyString)
  object CsvDecoderTestAddress extends OpenApiModel[CsvDecoderTestAddress] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestAddress] = DeriveSchema.gen
  }

  @entityName("CsvDecoderTestMessage")
  sealed trait CsvDecoderTestMessage
  object CsvDecoderTestMessage extends OpenApiModel[CsvDecoderTestMessage] {
    private val discriminator = new OpenApiDiscriminator[CsvDecoderTestMessage] {
      override val discriminatorFieldName = "type"
      override val mapping = Map(
        "text" -> "CsvDecoderTestTextMessage",
        "multimedia" -> "CsvDecoderTestMultimediaMessage"
      )
      override def discriminatorValue(obj: CsvDecoderTestMessage): String = obj match {
        case CsvDecoderTestTextMessage(_) => "text"
        case CsvDecoderTestMultimediaMessage(_) => "multimedia"
      }
    }
    implicit override lazy val zioSchema: Schema[CsvDecoderTestMessage] =
      DeriveSchema
        .gen[CsvDecoderTestMessage]
        .discriminator(discriminator)
  }

  @entityName("CsvDecoderTestTextMessage")
  final case class CsvDecoderTestTextMessage(text: String) extends CsvDecoderTestMessage
  object CsvDecoderTestTextMessage extends OpenApiModel[CsvDecoderTestTextMessage] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestTextMessage] = DeriveSchema.gen
  }

  @entityName("CsvDecoderTestMultimediaMessage")
  final case class CsvDecoderTestMultimediaMessage(url: List[String]) extends CsvDecoderTestMessage
  object CsvDecoderTestMultimediaMessage extends OpenApiModel[CsvDecoderTestMultimediaMessage] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestMultimediaMessage] = DeriveSchema.gen
  }

  @entityName("CsvDecoderTestItem")
  final case class CsvDecoderTestItem(price: BigDecimal)
  object CsvDecoderTestItem extends OpenApiModel[CsvDecoderTestItem] {
    implicit override lazy val zioSchema: Schema[CsvDecoderTestItem] = DeriveSchema.gen
  }
}

final class CsvDecoderTest extends AnyWordSpecLike with Assertions with Matchers {
  import eu.timepit.refined.auto._

  "CsvDecoder" should {
    val testPersonDecoder = CsvDecoder.derive[CsvDecoderTestPerson]()
    val testMessageDecoder = CsvDecoder.derive[CsvDecoderTestMessage]()
    val testItemDecoder = CsvDecoder.derive[CsvDecoderTestItem]()
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
      val result = testPersonDecoder.parse(row, None)
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
      val result = testPersonDecoder.parse(row, None)
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
      val error = CsvDecoderError("Cannot parse number.", Some("age"))
      val expected = Left(List(error))
      val result = testPersonDecoder.parse(row, None)
      assert(result == expected)
      assert(error.format == "Cannot parse number. Column(s): age")
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
      val result = testPersonDecoder.parse(row, None)
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
      val result = testPersonDecoder.parse(row, None)
      assert(result == expected)
    }

    "decode sum type" in {
      val row = Map(
        "type" -> "multimedia",
        "text" -> "",
        "url_1" -> "someurl1",
        "url_2" -> "someurl2"
      )
      val expected = CsvDecoderTestMultimediaMessage(
        url = List("someurl1", "someurl2")
      )
      val result = testMessageDecoder.parse(row, None)
      assert(result == Right(expected))
    }

    "fail decoding sum type if discriminator is incorrect" in {
      val row = Map(
        "type" -> "nonexisting",
        "text" -> "",
        "url_1" -> "someurl1",
        "url_2" -> "someurl2"
      )
      val expected =
        Left(List(CsvDecoderError("Unrecognized type: nonexisting. Valid object types are: multimedia, text.", None)))
      val result = testMessageDecoder.parse(row, None)
      assert(result == expected)
    }

    "decoder big decimals" in {
      val row = Map(
        "price" -> "5.30"
      )
      val expected = Right(CsvDecoderTestItem(BigDecimal("5.30")))
      val result = testItemDecoder.parse(row, None)
      assert(result == expected)
    }

  }
}
