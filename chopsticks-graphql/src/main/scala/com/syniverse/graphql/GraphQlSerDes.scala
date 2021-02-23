package dev.chopsticks.graphql

import caliban.client.CalibanClientError.DecodingError
import caliban.client.{__Value, SelectionBuilder}
import caliban.client.__Value.__ObjectValue

import scala.util.Try

object GraphQlSerDes {
  def deserialize[Origin, A](
    selection: SelectionBuilder[Origin, A],
    maybeData: Option[__Value]
  ): Either[DecodingError, A] = {
    for {
      objectValue <- maybeData match {
        case Some(o: __ObjectValue) => Right(o)
        case _ => Left(DecodingError("Result is not an object"))
      }
      result <- Try(fromGraphQL(selection, objectValue))
        .recover {
          case e =>
            Left(DecodingError("Unexpected error encountered during deserializing GraphQL message", Some(e)))
        }
        .get
    } yield result
  }

  private def fromGraphQL[Origin, A](selection: SelectionBuilder[Origin, A], value: __Value.__ObjectValue) = {
    selection match {
      case x: SelectionBuilder.Field[_, A] @unchecked => x.fromGraphQL(value)
      case x: SelectionBuilder.Mapping[_, Any, A] @unchecked => x.fromGraphQL(value)
      case x: SelectionBuilder.Concat[_, Any, Any] @unchecked =>
        x.fromGraphQL(value).asInstanceOf[Either[DecodingError, A]]
      case SelectionBuilder.Pure(a) => Right(a)
    }
  }

}
