package dev.chopsticks.fdb.util

import akka.actor.{ActorSystem, Status}
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import akka.{Done, NotUsed}
import com.apple.foundationdb._
import com.apple.foundationdb.tuple.Tuple
import dev.chopsticks.fdb.env.FdbEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.{LoggingContext, ZService}
import dev.chopsticks.stream.LastStateFlow
import zio.{URIO, ZIO}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

object FdbIterateSource extends LoggingContext {
  final case class IterateSourceAttempt[S](
    lastState: S,
    delay: FiniteDuration = Duration.Zero,
    lastFailure: Option[Throwable] = None
  )

  type MaybeLastKey = Option[Array[Byte]]
  type RetryOutput[S] = Either[Status.Status, IterateSourceAttempt[S]]

  def stopRetrying[S](s: Status.Status): RetryOutput[S] = Left(s)
  def retryWithAttempt[S](a: IterateSourceAttempt[S]): RetryOutput[S] = Right(a)

  private val lastStateFlow = Flow.fromGraph(
    LastStateFlow[KeyValue, Option[KeyValue], MaybeLastKey](
      seed = Option.empty[KeyValue],
      next = (_, b) => Some(b),
      result = _.map(_.getKey)
    )
  )

  def iterate(
    start: KeySelector,
    end: KeySelector,
    limit: Int = ReadTransaction.ROW_LIMIT_UNLIMITED,
    reverse: Boolean = false,
    streamingMode: StreamingMode = StreamingMode.ITERATOR,
    retryAfter: Option[FiniteDuration]
  ): ZIO[AkkaEnv with LogEnv with FdbEnv, Nothing, Source[KeyValue, NotUsed]] = {
    for {
      db <- ZService[FdbEnv.Service].map(_.database)
      createRequest = (attempt: IterateSourceAttempt[MaybeLastKey]) => {
        val maybeLastKey = attempt.lastState
        val lastFailure = attempt.lastFailure
        val lastKeyDisplay = Tuple.fromBytes(maybeLastKey.getOrElse(Array.emptyByteArray)).toString
        val startSelector = maybeLastKey.fold(start) { lastKey =>
          if (!reverse) KeySelector.firstGreaterThan(lastKey)
          else start
        }
        val endSelector = maybeLastKey.fold(end) { lastKey =>
          if (!reverse) end
          else KeySelector.firstGreaterOrEqual(lastKey)
        }

        val attemptInfo = lastFailure.map { f =>
          s"Retrying was-delayed=${attempt.delay} start=$startSelector end=$endSelector " +
            s"lastKey=$lastKeyDisplay " +
            s"lastFailure=${f.toString}"
        }

        val source = Source.fromGraph(
          new FdbIterateGraphStage(
            db,
            tx => {
              tx.getRange(startSelector, endSelector, limit, reverse, streamingMode)
                .iterator()
            }
          )
        )

        (attemptInfo, source)
      }
      source <- iterateSourceWithAutoRecovery[KeyValue, MaybeLastKey](
        Option.empty[Array[Byte]],
        createRequest,
        lastStateFlow
      ) { (priorState, state) =>
        {
          case Success(v) =>
            retryAfter match {
              case Some(duration) =>
                retryWithAttempt(IterateSourceAttempt(state.orElse(priorState), duration, None))
              case None =>
                stopRetrying(Status.Success(v))
            }

          case Failure(e: FDBException) =>
            retryWithAttempt(IterateSourceAttempt(state.orElse(priorState), Duration.Zero, Some(e)))

          case Failure(e) =>
            stopRetrying(Status.Failure(e))
        }
      }
    } yield source
  }

  def iterateSourceWithAutoRecovery[B, S](
    initialState: => S,
    createRequest: IterateSourceAttempt[S] => (
      Option[String],
      Source[B, NotUsed]
    ),
    lastStateFlow: Flow[B, B, Future[(S, Try[Done])]]
  )(
    retryMatcher: (S, S) => PartialFunction[Try[Done], RetryOutput[S]]
  ): URIO[AkkaEnv with LogEnv, Source[B, NotUsed]] = {
    for {
      actorSystem <- ZService[AkkaEnv.Service].map(_.actorSystem)
      logger <- ZService[LogEnv.Service].map(_.logger)
    } yield {
      implicit val as: ActorSystem = actorSystem
      import as.dispatcher

      Source
        .lazyFuture(() => {
          val (queue, attemptSource) = Source
            .queue[IterateSourceAttempt[S]](1, OverflowStrategy.fail)
            .preMaterialize()

          def enqueue(attempt: IterateSourceAttempt[S]): Future[Done] = {
            queue.offer(attempt).flatMap {
              case QueueOfferResult.Enqueued =>
                Future.successful(Done)

              case QueueOfferResult.Dropped =>
                queue.fail(new RuntimeException(s"Failed enqueuing attempt: $attempt due to: Dropped"))
                queue.watchCompletion()

              case QueueOfferResult.Failure(cause) =>
                queue.fail(cause)
                queue.watchCompletion()

              case QueueOfferResult.QueueClosed =>
                Future.failed(new RuntimeException(s"Failed enqueuing attempt: $attempt due to: QueueClosed"))
            }
          }

          enqueue(IterateSourceAttempt[S](initialState)).map {
            _ =>
              attemptSource
                .mapAsync(1) { attempt =>
                  val fa = Future.successful(attempt)
                  if (attempt.delay > Duration.Zero) akka.pattern.after(attempt.delay, as.scheduler)(fa)
                  else fa
                }
                .flatMapConcat {
                  attempt =>
                    val (maybeLog, source) = createRequest(attempt)

                    maybeLog.foreach(m => logger.warn(m))

                    source
                      .viaMat(lastStateFlow)(Keep.right)
                      .watchTermination() {
                        case (lastStateFuture: Future[(S, Try[Done])], upstreamFuture: Future[Done]) =>
                          val f = for {
                            lastStateResult <- lastStateFuture
                            toSend <- upstreamFuture.transformWith { t =>
                              val (lastState, maybeException) = lastStateResult
                              Future
                                .successful(retryMatcher(attempt.lastState, lastState)(maybeException.flatMap(_ => t)))
                            }
                          } yield toSend

                          f.transformWith {
                            case Failure(ex) =>
                              queue.fail(ex)
                              queue.watchCompletion()

                            case Success(Left(s)) =>
                              s match {
                                case Status.Success(_) => queue.complete()
                                case Status.Failure(cause) => queue.fail(cause)
                              }
                              queue.watchCompletion()

                            case Success(Right(s)) =>
                              enqueue(s)
                          }
                      }
                }
          }
        })
        .flatMapConcat(identity)
    }
  }

}
