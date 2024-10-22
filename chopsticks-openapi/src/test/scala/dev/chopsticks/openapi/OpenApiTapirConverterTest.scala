package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiAnnotations.entityName
import dev.chopsticks.openapi.OpenApiTapirJsonCirce.jsonBody
import dev.chopsticks.openapi.OpenApiZioSchemas.ZioSchemaOps
import enumeratum.EnumEntry
import org.scalatest.matchers.should.Matchers
import org.scalatest.Assertions
import org.scalatest.wordspec.AnyWordSpecLike
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe.yaml.RichOpenAPI
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import zio.schema.{DeriveSchema, Schema}
import zio.Chunk

import java.time.Instant

object OpenApiTapirConverterTest {
  sealed trait ValidityTimeFormatType extends EnumEntry

  object ValidityTimeFormatType extends enumeratum.Enum[ValidityTimeFormatType] {
    final case object Absolute extends ValidityTimeFormatType

    final case object Relative extends ValidityTimeFormatType

    override lazy val values: IndexedSeq[ValidityTimeFormatType] = findValues
  }

  @entityName("ValidityTimeFormat")
  sealed trait ValidityTimeFormat extends Product with Serializable

  object ValidityTimeFormat extends OpenApiModel[ValidityTimeFormat] {
    @entityName("ValidityTimeFormat_Absolute")
    final case class Absolute(absolute: Instant) extends ValidityTimeFormat
    @entityName("ValidityTimeFormat_Relative")
    final case class Relative(relativeSeconds: Long) extends ValidityTimeFormat

    implicit lazy val discriminator = new OpenApiDiscriminator[ValidityTimeFormat] {
      override val discriminatorFieldName: String = "type"

      override lazy val mapping: Map[String, String] = {
        ValidityTimeFormatType
          .values
          .map {
            case e @ ValidityTimeFormatType.Absolute =>
              e.entryName -> "ValidityTimeFormat_Absolute"
            case e @ ValidityTimeFormatType.Relative =>
              e.entryName -> "ValidityTimeFormat_Relative"
          }
          .toMap
      }

      override def discriminatorValue(obj: ValidityTimeFormat): String = obj match {
        case _: Absolute => ValidityTimeFormatType.Absolute.entryName
        case _: Relative => ValidityTimeFormatType.Relative.entryName
      }
    }

    implicit lazy val zioSchema: Schema[ValidityTimeFormat] = {
      DeriveSchema
        .gen[ValidityTimeFormat]
        .discriminator(discriminator)
    }
  }

  sealed trait MessageType extends EnumEntry
  object MessageType extends enumeratum.Enum[MessageType] {
    final case object Text extends MessageType
    final case object Email extends MessageType

    override lazy val values: IndexedSeq[MessageType] = findValues
  }

  sealed trait Message extends Product with Serializable
  object Message extends OpenApiModel[Message] {
    final case class Text(text: String, validityTime: ValidityTimeFormat) extends Message
    final case class Email(email: String, validityTime: ValidityTimeFormat) extends Message

    implicit val discriminator = new OpenApiDiscriminator[Message] {
      override val discriminatorFieldName: String = "type"
      override val mapping: Map[String, String] = MessageType.values.map {
        case e @ MessageType.Text => e.entryName -> "OpenApiTapirConverterTest_Message_Text"
        case e @ MessageType.Email => e.entryName -> "OpenApiTapirConverterTest_Message_Email"
      }.toMap
      override def discriminatorValue(obj: Message): String = obj match {
        case _: Text => MessageType.Text.entryName
        case _: Email => MessageType.Email.entryName
      }
    }

    implicit lazy val zioSchema: Schema[Message] = DeriveSchema
      .gen[Message]
      .discriminator(discriminator)
  }

  final case class Messages(messages: Chunk[Message])
  object Messages extends OpenApiModel[Messages] {
    implicit lazy val zioSchema: Schema[Messages] = DeriveSchema.gen
  }

}

final class OpenApiTapirConverterTest extends AnyWordSpecLike with Assertions with Matchers {
  import OpenApiTapirConverterTest._
  import sttp.tapir._

  "OpenApiTapirConverter" should {
    "generate the correct OpenAPI YAML for TimeFormat" in {
      val e = endpoint.in("messages").out(jsonBody[Messages])
      val openApi: OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(List(e), "TimeFormat API", "1.0")
      val actualYaml = openApi.toYaml

      val expectedYaml =
        """openapi: 3.1.0
          |info:
          |  title: TimeFormat API
          |  version: '1.0'
          |paths:
          |  /messages:
          |    get:
          |      operationId: getMessages
          |      responses:
          |        '200':
          |          description: ''
          |          content:
          |            application/json:
          |              schema:
          |                $ref: '#/components/schemas/OpenApiTapirConverterTest_Messages'
          |components:
          |  schemas:
          |    OpenApiTapirConverterTest_Message:
          |      oneOf:
          |      - $ref: '#/components/schemas/OpenApiTapirConverterTest_Message_Email'
          |      - $ref: '#/components/schemas/OpenApiTapirConverterTest_Message_Text'
          |      discriminator:
          |        propertyName: type
          |        mapping:
          |          Email: '#/components/schemas/OpenApiTapirConverterTest_Message_Email'
          |          Text: '#/components/schemas/OpenApiTapirConverterTest_Message_Text'
          |    OpenApiTapirConverterTest_Message_Email:
          |      required:
          |      - email
          |      - validityTime
          |      type: object
          |      properties:
          |        email:
          |          type: string
          |        validityTime:
          |          $ref: '#/components/schemas/ValidityTimeFormat'
          |    OpenApiTapirConverterTest_Message_Text:
          |      required:
          |      - text
          |      - validityTime
          |      type: object
          |      properties:
          |        text:
          |          type: string
          |        validityTime:
          |          $ref: '#/components/schemas/ValidityTimeFormat'
          |    OpenApiTapirConverterTest_Messages:
          |      required:
          |      - messages
          |      type: object
          |      properties:
          |        messages:
          |          type: array
          |          items:
          |            $ref: '#/components/schemas/OpenApiTapirConverterTest_Message'
          |    ValidityTimeFormat:
          |      oneOf:
          |      - $ref: '#/components/schemas/ValidityTimeFormat_Absolute'
          |      - $ref: '#/components/schemas/ValidityTimeFormat_Relative'
          |      discriminator:
          |        propertyName: type
          |        mapping:
          |          Absolute: '#/components/schemas/ValidityTimeFormat_Absolute'
          |          Relative: '#/components/schemas/ValidityTimeFormat_Relative'
          |    ValidityTimeFormat_Absolute:
          |      required:
          |      - absolute
          |      type: object
          |      properties:
          |        absolute:
          |          type: string
          |          format: date-time
          |    ValidityTimeFormat_Relative:
          |      required:
          |      - relativeSeconds
          |      type: object
          |      properties:
          |        relativeSeconds:
          |          type: number
          |""".stripMargin

      actualYaml shouldBe expectedYaml
    }
  }
}
