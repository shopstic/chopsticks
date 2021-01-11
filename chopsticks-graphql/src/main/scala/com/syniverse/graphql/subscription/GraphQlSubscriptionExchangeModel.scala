package dev.chopsticks.graphql.subscription

import caliban.client.GraphQLRequest
import caliban.client.Operations.RootSubscription
import caliban.client.{GraphQLResponse, SelectionBuilder}
import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation._

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

  object GraphQlSubscriptionProtocolClientMessage {
    // todo add payload
    final case class GraphQlSubscriptionConnectionInit private (`type`: String)
        extends GraphQlSubscriptionProtocolClientMessage
    object GraphQlSubscriptionConnectionInit {
      val Type = "connection_init"
      def apply(): GraphQlSubscriptionConnectionInit = GraphQlSubscriptionConnectionInit(Type)

      implicit lazy val encoder: Encoder[GraphQlSubscriptionConnectionInit] =
        deriveEncoder[GraphQlSubscriptionConnectionInit]
    }

    final case class GraphQlSubscriptionStart private (id: String, `type`: String, payload: Json)
        extends GraphQlSubscriptionProtocolClientMessage
    object GraphQlSubscriptionStart {
      val Type = "start"
      def apply[A](id: String, payload: SelectionBuilder[RootSubscription, A]): GraphQlSubscriptionStart = {
        val payloadAsJson = implicitly[Encoder[GraphQLRequest]].apply(payload.toGraphQL(useVariables = true))
        GraphQlSubscriptionStart(id, Type, payloadAsJson)
      }

      implicit lazy val encoder: Encoder[GraphQlSubscriptionStart] = deriveEncoder[GraphQlSubscriptionStart]
    }

    implicit lazy val encoder: Encoder[GraphQlSubscriptionProtocolClientMessage] =
      Encoder.instance[GraphQlSubscriptionProtocolClientMessage] {
        x => GraphQlSubscriptionProtocolClientMessage.encoder.apply(x)
      }
  }

  object GraphQlSubscriptionProtocolServerMessage {
    final case class GraphQlConnectionError(`type`: String, payload: Json)
        extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionError {
      val Type = "connection_error"
      implicit lazy val decoder: Decoder[GraphQlConnectionError] = deriveDecoder[GraphQlConnectionError]
    }

    final case class GraphQlConnectionAck(`type`: String) extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionAck {
      val Type = "connection_ack"
      implicit lazy val decoder: Decoder[GraphQlConnectionAck] = deriveDecoder[GraphQlConnectionAck]
    }

    final case class GraphQlConnectionData(id: String, `type`: String, payload: GraphQLResponse)
        extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionData {
      val Type = "data"
      implicit lazy val decoder: Decoder[GraphQlConnectionData] = deriveDecoder[GraphQlConnectionData]
    }

    final case class GraphQlConnectionKeepAlive(`type`: String) extends GraphQlSubscriptionProtocolServerMessage
    object GraphQlConnectionKeepAlive {
      val Type = "ka"
      implicit lazy val decoder: Decoder[GraphQlConnectionKeepAlive] = deriveDecoder[GraphQlConnectionKeepAlive]
    }

    implicit lazy val decoder: Decoder[GraphQlSubscriptionProtocolServerMessage] = { cursor =>
      for {
        tpe <- cursor.downField("type").as[String]
        result <- tpe match {
          case GraphQlConnectionError.Type => GraphQlConnectionError.decoder(cursor)
          case GraphQlConnectionAck.Type => GraphQlConnectionAck.decoder(cursor)
          case GraphQlConnectionData.Type => GraphQlConnectionData.decoder(cursor)
          case GraphQlConnectionKeepAlive.Type => GraphQlConnectionKeepAlive.decoder(cursor)
          case _ => ???
        }
      } yield result
    }
  }
}
