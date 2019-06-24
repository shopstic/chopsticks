package dev.chopsticks.kvdb.util

import akka.http.scaladsl.marshalling.{Marshaller, PredefinedToEntityMarshallers, ToEntityMarshaller}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

object AkkaHttpProtobufSupport {
  implicit def protobufMarshaller[T <: GeneratedMessage]: ToEntityMarshaller[T] =
    PredefinedToEntityMarshallers.ByteArrayMarshaller.compose[T](_.toByteArray)

  implicit def protobufUnmarshaller[T <: GeneratedMessage with Message[T]](
    implicit companion: GeneratedMessageCompanion[T]
  ): FromEntityUnmarshaller[T] =
    Unmarshaller.byteArrayUnmarshaller.map[T](companion.parseFrom)

  implicit val emptyMarshaller: ToEntityMarshaller[Unit] =
    Marshaller.withFixedContentType(ContentTypes.NoContentType)(_ => HttpEntity.Empty)
}
