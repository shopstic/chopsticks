package dev.chopsticks.kvdb

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import dev.chopsticks.proto.db.DbOperationException.ExceptionType
import dev.chopsticks.proto.db.DbOperationPossibleException
import dev.chopsticks.util.DbUtils._

import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

object DbWebSocketClientResponseByteStringHandler {
  object UpstreamCompletedBeforeExplicitCompletionSignalException extends RuntimeException with NoStackTrace
}

final class DbWebSocketClientResponseByteStringHandler extends GraphStage[FlowShape[ByteString, ByteString]] {
  import DbWebSocketClientResponseByteStringHandler._

  val inlet: Inlet[ByteString] = Inlet[ByteString]("RocksdbWebSocketClientResponseHandler.in")
  val outlet: Outlet[ByteString] = Outlet[ByteString]("RocksdbWebSocketClientResponseHandler.out")

  override val shape = FlowShape(inlet, outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var completed = false
      private def isCompletionMessage(message: ByteString): Boolean = message.isEmpty

      private lazy val streamingHandler = new InHandler {
        override def onUpstreamFinish(): Unit = {
          if (!completed) {
            failStage(UpstreamCompletedBeforeExplicitCompletionSignalException)
          }
        }

        def onPush(): Unit = {
          val message = grab(inlet)
          if (!isCompletionMessage(message)) {
            emit(outlet, message)
          }
          else {
            completed = true
            completeStage()
          }
        }
      }

      setHandler(
        inlet,
        new InHandler {
          override def onUpstreamFinish(): Unit = {
            if (!completed) {
              failStage(UpstreamCompletedBeforeExplicitCompletionSignalException)
            }
          }

          def onPush(): Unit = {
            val message = grab(inlet)

            if (!isCompletionMessage(message)) {
              Try {
                DbOperationPossibleException.parseFrom(message.toArray).content match {
                  case DbOperationPossibleException.Content.Exception(ex) =>
                    val message = ex.message
                    val convertedEx = ex.exceptionType match {
                      case ExceptionType.SEEK_FAILURE => SeekFailure(message)
                      case ExceptionType.UNOPTIMIZED_OPERATION => UnoptimizedDbOperationException(message)
                      case ExceptionType.INVALID_COLUMN_FAMILY => InvalidDbColumnFamilyException(message)
                      case ExceptionType.INVALID_ARGUMENT => InvalidDbArgumentException(message)
                      case ExceptionType.UNSUPPORTED_OPERATION => UnsupportedDbOperationException(message)
                      case ExceptionType.ALREADY_CLOSED => DbAlreadyClosedException(message)
                      case ExceptionType.Unrecognized(value) =>
                        new IllegalStateException(
                          s"Got DbOperationException but its exceptionType value of $value is unrecognizable"
                        )
                    }
                    throw convertedEx

                  case DbOperationPossibleException.Content.Message(bytes) => ByteString(bytes.toByteArray)

                  case _ => throw new IllegalStateException(s"Got DbOperationPossibleException with empty content")
                }
              } match {
                case Success(out) =>
                  setHandler(inlet, streamingHandler)
                  emit(outlet, out)

                case Failure(ex) => failStage(ex)
              }
            }
            else {
              completed = true
              completeStage()
            }
          }
        }
      )

      setHandler(outlet, new OutHandler {
        def onPull(): Unit = {
          tryPull(inlet)
        }
      })
    }
}
