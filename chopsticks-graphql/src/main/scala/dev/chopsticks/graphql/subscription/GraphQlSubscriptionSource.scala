package dev.chopsticks.graphql.subscription

import java.util.concurrent.TimeoutException
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import org.apache.pekko.stream.scaladsl.{BidiFlow, Flow, Keep, RestartSource, Sink, Source}
import org.apache.pekko.stream.stage._
import org.apache.pekko.stream._
import caliban.client.CalibanClientError.ServerError
import caliban.client.Operations.RootSubscription
import caliban.client.SelectionBuilder
import dev.chopsticks.graphql.GraphQlSerDes
import dev.chopsticks.graphql.subscription.GraphQlSubscriptionException.{
  GraphQlDataErrorsException,
  GraphQlNonRetryableException,
  GraphQlOtherException
}
import dev.chopsticks.graphql.subscription.GraphQlSubscriptionExchangeModel.GraphQlSubscriptionProtocolClientMessage.{
  GraphQlSubscriptionConnectionInit,
  GraphQlSubscriptionStart
}
import dev.chopsticks.graphql.subscription.GraphQlSubscriptionExchangeModel.{
  GraphQlSubscriptionProtocolClientMessage,
  GraphQlSubscriptionProtocolServerMessage
}
import dev.chopsticks.graphql.subscription.GraphQlSubscriptionExchangeModel.GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionData
import dev.chopsticks.stream.GraphStageWithActorLogic

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success, Try}

object ConditionalRestartSource {

  /** Restarts source based on the `shouldRestart` function. Warning: cancelling restarts happens concurrently, so if
    * backoff elapses instantly, there may arise situation when stream gets restarted one more time and right after that
    * (concurrently) it will get shutdown.
    */
  def restartSource[A, Mat](
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    idleTimeout: Option[FiniteDuration]
  )(
    sourceFactory: () => Source[A, Mat]
  )(shouldRestart: Try[Mat] => Boolean)(implicit mat: Materializer): Source[A, Future[Mat]] = {
    import mat.executionContext
    val promise = Promise[Mat]()
    val (killSwitch, source) =
      RestartSource
        .withBackoff(RestartSettings(minBackoff, maxBackoff, randomFactor)) { () =>
          sourceFactory()
            .alsoToMat(Sink.ignore)(Keep.both)
            .withAttributes(
              Attributes(
                Attributes.CancellationStrategy(Attributes.cancellationStrategyCompleteState) :: Nil
              )
            )
            .via(idleTimeout.fold(Flow[A])(timeout => Flow[A].idleTimeout(timeout)))
            .recoverWithRetries(
              1,
              {
                case _: TimeoutException => Source.empty
              }
            )
            .mapMaterializedValue {
              case (mat, futureDone) =>
                futureDone.map(_ => mat).onComplete { tryMat =>
                  if (!shouldRestart(tryMat)) {
                    val _ = promise.complete(tryMat)
                  }
                }
            }
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .preMaterialize()

    promise.future.onComplete {
      case Success(_) => killSwitch.shutdown()
      case Failure(e) => killSwitch.abort(e)
    }

    source.mapMaterializedValue { _ => promise.future }
  }

}

object GraphQlSubscriptionSource {
  def apply[A](
    uri: Uri,
    subscription: SelectionBuilder[RootSubscription, A],
    idleTimeout: Option[FiniteDuration],
    minRestartBackoff: FiniteDuration = 50.millis,
    maxRestartBackoff: FiniteDuration = 250.millis
  )(implicit actorSystem: ActorSystem): Source[A, Future[Unit]] = {
    apply(
      id = Random.alphanumeric.take(10).mkString(""),
      uri = uri,
      subscription = subscription,
      idleTimeout = idleTimeout,
      minRestartBackoff = minRestartBackoff,
      maxRestartBackoff = maxRestartBackoff
    )
  }

  def apply[A](
    id: String,
    uri: Uri,
    subscription: SelectionBuilder[RootSubscription, A],
    idleTimeout: Option[FiniteDuration],
    minRestartBackoff: FiniteDuration,
    maxRestartBackoff: FiniteDuration
  )(implicit actorSystem: ActorSystem): Source[A, Future[Unit]] = {
    import actorSystem.dispatcher
    ConditionalRestartSource
      .restartSource(minRestartBackoff, maxRestartBackoff, 0.1, idleTimeout) { () =>
        val wsFlow = Http().webSocketClientFlow(WebSocketRequest(uri, subprotocol = Some("graphql-ws")))
        val subscriptionBidiFlow = new GraphQlSubscriptionBidiFlow[A](id, subscription)
        val flow =
          BidiFlow
            .fromGraph(subscriptionBidiFlow)
            .join(wsFlow)
        Source.single(()).via(flow)
      } {
        case Failure(e: GraphQlNonRetryableException) =>
          actorSystem.log.error("GraphQlSubscriptionSource has failed. Stopping the source.", e)
          false
        case _ => true
      }
      .mapMaterializedValue { _.map(_ => ()) }
  }
}

private[graphql] class GraphQlSubscriptionBidiFlow[A](id: String, subscription: SelectionBuilder[RootSubscription, A])
    extends GraphStage[BidiShape[Unit, Message, Message, A]] {

  private val stageName = "GraphQlSubscriptionBidiFlow"
  private val messagesIn = Inlet[Message](s"$stageName.messages.in")
  private val messagesOut = Outlet[Message](s"$stageName.messages.out")
  private val nothingIn = Inlet[Unit](s"$stageName.nothing.in")
  private val subscriptionOut = Outlet[A](s"$stageName.subscription.out")

  override val shape = BidiShape.of(nothingIn, messagesOut, messagesIn, subscriptionOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with GraphStageWithActorLogic { logic =>
      implicit lazy val mat = logic.materializer
      implicit lazy val ec = materializer.executionContext
      var behavior: Behavior = Behavior.Uninitialized
      val strictMessageCallback: AsyncCallback[TextMessage.Strict] = getAsyncCallback[TextMessage.Strict] {
        this.behavior.onConnectionPushStrictMessage
      }
      val failStageCallback: AsyncCallback[Throwable] = getAsyncCallback[Throwable](failStage)
      val queue = scala.collection.mutable.Queue[A]()

      setHandler(messagesIn, inHandler(() => behavior.onConnectionPush()))
      setHandler(messagesOut, outHandler(() => behavior.onConnectionPull()))
      setHandler(nothingIn, inHandler(() => ()))
      setHandler(subscriptionOut, outHandler(() => behavior.onSubscriptionPull()))

      private def outHandler(onPullElem: () => Unit): OutHandler = () => onPullElem()
      private def inHandler(onPushElem: () => Unit): InHandler = () => onPushElem()

      sealed trait Behavior extends Product with Serializable {
        def onConnectionPull(): Unit
        def onConnectionPush(): Unit = {
          val elem: Message = logic.grab(messagesIn)
          elem match {
            case m: TextMessage.Strict => this.onConnectionPushStrictMessage(m)
            case m: TextMessage =>
              m.toStrict(10.seconds).onComplete {
                case Success(strict) => strictMessageCallback.invoke(strict)
                case Failure(e) =>
                  val exception = new RuntimeException(
                    s"$stageName in state $this failed during converting message to strict message",
                    e
                  )
                  failStageCallback.invoke(exception)
              }
            case x =>
              logic.failStage(
                new IllegalArgumentException(
                  s"$stageName only accepts TextMessages, failed in state $this got message: $x"
                )
              )
          }
        }
        def onConnectionPushStrictMessage(strict: TextMessage.Strict): Unit = {
          import com.github.plokhotnyuk.jsoniter_scala.core._
          Try(readFromString[GraphQlSubscriptionProtocolServerMessage](strict.text)) match {
            case Failure(e) =>
              val exception =
                new RuntimeException(s"$stageName in state $this failed during decoding message", e)
              logic.failStage(exception)
            case Success(message) => onConnectionPushMessage(message)
          }
        }
        def onConnectionPushMessage(message: GraphQlSubscriptionProtocolServerMessage): Unit
        def onSubscriptionPull(): Unit
        protected def transition(toBehavior: Behavior): Unit = {
          log.debug(s"Transitioning to $toBehavior")
          logic.behavior = toBehavior
        }
      }

      object Behavior {

        final case object Uninitialized extends Behavior {
          override def onConnectionPull(): Unit = {
            logic.push(
              messagesOut,
              TextMessage.Strict(
                GraphQlSubscriptionProtocolClientMessage.serialize(GraphQlSubscriptionConnectionInit())
              )
            )
            this.transition(PendingInitialization())
          }
          override def onConnectionPushMessage(message: GraphQlSubscriptionProtocolServerMessage): Unit = {
            // when there is big collection of messages,
            // Hasura doesn't wait for whole initialization process to complete and sends data response directly
            message match {
              case x: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionData =>
                transition(Behavior.Initialized(Some(x)))
              case other =>
                val exception = new GraphQlOtherException(
                  s"$stageName failed in state $this. Got unexpected message: $other"
                )
                logic.failStage(exception)
            }
          }
          override def onSubscriptionPull(): Unit = ()
        }

        sealed case class PendingInitialization() extends Behavior {
          logic.pull(messagesIn)
          override def onConnectionPull(): Unit = {
            if (!logic.hasBeenPulled(messagesIn)) logic.pull(messagesIn)
          }

          override def onConnectionPushMessage(message: GraphQlSubscriptionProtocolServerMessage): Unit = {
            message match {
              case _: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionAck =>
                transition(Initialized(None))
              case x: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionError =>
                val exception = new GraphQlOtherException(s"$stageName failed in state $this. Got connection error: $x")
                logic.failStage(exception)
              case _: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionKeepAlive =>
                logic.pull(messagesIn)
              case x: GraphQlConnectionData =>
                transition(Initialized(Some(x)))
            }
          }

          override def onSubscriptionPull(): Unit = ()
        }

        sealed case class Initialized(dataMessage: Option[GraphQlConnectionData]) extends Behavior {
          dataMessage match {
            case Some(x) => onConnectionPushMessage(x)
            case None =>
              val message = TextMessage.Strict(
                GraphQlSubscriptionProtocolClientMessage.serialize(GraphQlSubscriptionStart(id, subscription))
              )
              logic.push(messagesOut, message)
              logic.pull(messagesIn)
          }
          override def onConnectionPull(): Unit = {
            if (!logic.hasBeenPulled(messagesIn)) logic.pull(messagesIn)
          }
          override def onConnectionPushMessage(message: GraphQlSubscriptionProtocolServerMessage): Unit = {
            message match {
              case _: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionKeepAlive =>
                logic.pull(messagesIn)
              case data: GraphQlSubscriptionProtocolServerMessage.GraphQlConnectionData =>
                parseResponse(data) match {
                  case Right(value) =>
                    val _ = queue.enqueue(value)
                    if (logic.isAvailable(subscriptionOut)) {
                      onSubscriptionPull()
                    }
                    logic.pull(messagesIn)
                  case Left(error) =>
                    failStage(error)
                }
              case other =>
                log.warning(s"$stageName in state $this got unexpected connection data: $other")
                logic.pull(messagesIn)
            }
          }

          override def onSubscriptionPull(): Unit = {
            if (queue.nonEmpty) logic.push(subscriptionOut, queue.dequeue())
          }

          private def parseResponse(parsed: GraphQlConnectionData): Either[GraphQlSubscriptionException, A] = {
            for {
              maybeData <- {
                if (parsed.payload.errors.nonEmpty) {
                  if (parsed.payload.errors.exists(_.message == "connection error")) {
                    Left(GraphQlOtherException("Upstream server experienced connection error."))
                  }
                  else {
                    Left(GraphQlDataErrorsException(ServerError(parsed.payload.errors)))
                  }
                }
                else Right(parsed.payload.data)
              }
              result <- GraphQlSerDes.deserialize(subscription, maybeData).left.map(GraphQlOtherException(_))
            } yield result
          }
        }

      }
    }
  }
}
