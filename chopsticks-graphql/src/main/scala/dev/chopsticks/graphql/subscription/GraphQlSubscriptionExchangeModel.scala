package dev.chopsticks.graphql.subscription

import caliban.client.GraphQLRequest
import caliban.client.Operations.RootSubscription
import caliban.client.{GraphQLResponse, SelectionBuilder}
import io.circe.Json
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import java.nio.charset.StandardCharsets

sealed abstract class GraphQlSubscriptionException(message: String, cause: Option[Throwable] = Option.empty)
    extends RuntimeException(message, cause.orNull)

object GraphQlSubscriptionException {
  sealed abstract class GraphQlRetryableException(message: String, cause: Option[Throwable] = Option.empty)
      extends GraphQlSubscriptionException(message, cause)
  sealed abstract class GraphQlNonRetryableException(message: String, cause: Option[Throwable] = Option.empty)
      extends GraphQlSubscriptionException(message, cause)

  final case class GraphQlDataErrorsException(message: String, cause: Option[Throwable] = Option.empty)
      extends GraphQlNonRetryableException(message)
  object GraphQlDataErrorsException {
    def apply(cause: Throwable): GraphQlDataErrorsException =
      GraphQlDataErrorsException(cause.getMessage, Some(cause))
  }

  final case class GraphQlOtherException(message: String, cause: Option[Throwable] = Option.empty)
      extends GraphQlRetryableException(message, cause)

  object GraphQlOtherException {
    def apply(cause: Throwable): GraphQlOtherException =
      GraphQlOtherException(cause.getMessage, Some(cause))
  }
}

private[subscription] object GraphQlSubscriptionExchangeModel {
  sealed trait GraphQlSubscriptionProtocolMessage extends Product with Serializable {
    def `type`: String
  }

  sealed trait GraphQlSubscriptionProtocolClientMessage extends GraphQlSubscriptionProtocolMessage
  sealed trait GraphQlSubscriptionProtocolServerMessage extends GraphQlSubscriptionProtocolMessage

  implicit val circeJsonCodec: JsonValueCodec[io.circe.Json] = new JsonValueCodec[io.circe.Json] {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import io.circe.parser._

    override def decodeValue(in: JsonReader, default: io.circe.Json): io.circe.Json = {
      // Parse the JSON string into a Circe Json object
      val jsonString = new String(in.readRawValAsBytes(), StandardCharsets.UTF_8)
      parse(jsonString).getOrElse(io.circe.Json.Null)
    }

    override def encodeValue(x: io.circe.Json, out: JsonWriter): Unit = {
      // Convert the Circe Json object to a string and write it using JsonWriter
      out.writeRawVal(x.noSpaces.getBytes("UTF-8"))
    }

    override def nullValue: io.circe.Json = io.circe.Json.Null
  }

  object GraphQlSubscriptionProtocolClientMessage {
    // todo add payload
    @named("connection_init") final case class GraphQlSubscriptionConnectionInit private (
      `type`: String = GraphQlSubscriptionConnectionInit.Type
    ) extends GraphQlSubscriptionProtocolClientMessage
    object GraphQlSubscriptionConnectionInit {
      val Type = "connection_init"
//      def apply(): GraphQlSubscriptionConnectionInit = GraphQlSubscriptionConnectionInit(Type)

      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlSubscriptionConnectionInit]
    }

    @named("start") final case class GraphQlSubscriptionStart private (
      id: String,
      `type`: String = GraphQlSubscriptionStart.Type,
      payload: GraphQLRequest
    ) extends GraphQlSubscriptionProtocolClientMessage
    object GraphQlSubscriptionStart {
      val Type = "start"
      def apply[A](id: String, payload: SelectionBuilder[RootSubscription, A]): GraphQlSubscriptionStart = {
//        val payloadAsJson = implicitly[Encoder[GraphQLRequest]].apply(payload.toGraphQL(useVariables = true))
        GraphQlSubscriptionStart(id = id, payload = payload.toGraphQL(dropNullInputValues = true, useVariables = true))
      }

      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlSubscriptionStart]
    }

    implicit lazy val codec = JsonCodecMaker.make[GraphQlSubscriptionProtocolClientMessage](
      CodecMakerConfig.withDiscriminatorFieldName(Some("type"))
    )
    def serialize(msg: GraphQlSubscriptionProtocolClientMessage): String = {
      writeToString(msg)
    }
  }

  object GraphQlSubscriptionProtocolServerMessage {
    @named("connection_error") final case class GraphQlConnectionError(
      `type`: String = GraphQlConnectionError.Type,
      payload: Json
    ) extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionError {
      val Type = "connection_error"
//      implicit lazy val decoder: Decoder[GraphQlConnectionError] = deriveDecoder[GraphQlConnectionError]
      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlConnectionError]
    }

    @named("connection_ack") final case class GraphQlConnectionAck(`type`: String = GraphQlConnectionAck.Type)
        extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionAck {
      val Type = "connection_ack"
//      implicit lazy val decoder: Decoder[GraphQlConnectionAck] = deriveDecoder[GraphQlConnectionAck]
      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlConnectionAck]
    }

    @named("data") final case class GraphQlConnectionData(
      id: String,
      `type`: String = GraphQlConnectionData.Type,
      payload: GraphQLResponse
    ) extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionData {
      val Type = "data"
//      implicit lazy val decoder: Decoder[GraphQlConnectionData] = deriveDecoder[GraphQlConnectionData]
      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlConnectionData]
    }

    @named("ka") final case class GraphQlConnectionKeepAlive(`type`: String = GraphQlConnectionKeepAlive.Type)
        extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionKeepAlive {
      val Type = "ka"
//      implicit lazy val decoder: Decoder[GraphQlConnectionKeepAlive] = deriveDecoder[GraphQlConnectionKeepAlive]
      implicit lazy val codec = JsonCodecMaker.makeCirceLike[GraphQlConnectionKeepAlive]
    }

    implicit lazy val codec = JsonCodecMaker.make[GraphQlSubscriptionProtocolServerMessage](
      CodecMakerConfig.withDiscriminatorFieldName(Some("type"))
    )

//    implicit lazy val decoder: Decoder[GraphQlSubscriptionProtocolServerMessage] = { cursor =>
//      for {
//        tpe <- cursor.downField("type").as[String]
//        result <- tpe match {
//          case GraphQlConnectionError.Type => GraphQlConnectionError.decoder(cursor)
//          case GraphQlConnectionAck.Type => GraphQlConnectionAck.decoder(cursor)
//          case GraphQlConnectionData.Type => GraphQlConnectionData.decoder(cursor)
//          case GraphQlConnectionKeepAlive.Type => GraphQlConnectionKeepAlive.decoder(cursor)
//          case _ => ???
//        }
//      } yield result
//    }
  }
}
