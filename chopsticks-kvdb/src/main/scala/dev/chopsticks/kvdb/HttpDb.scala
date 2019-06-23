package dev.chopsticks.kvdb

import java.util.UUID

import akka.actor.{ActorSystem, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.{Deflate, Gzip}
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.headers.{`Content-Encoding`, HttpEncodings, RawHeader}
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, RequestEntity, StatusCode, Uri}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, StreamTcpException}
import akka.util.ByteString
import akka.{Done, NotUsed}
import cats.syntax.show._
import com.google.protobuf.{CodedInputStream, ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.codec.DbKeyConstraints.Implicits._
import dev.chopsticks.kvdb.DbInterface._
import dev.chopsticks.kvdb.DbWebSocketClientResponseByteStringHandler.UpstreamCompletedBeforeExplicitCompletionSignalException
import dev.chopsticks.kvdb.HttpDbServer.VALIDATION_REJECTION_REASON_HEADER_NAME
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.proto.db.DbTailItemValueBatch.Value
import dev.chopsticks.proto.db._
import dev.chopsticks.stream.{AkkaStreamUtils, LastStateFlow}
import dev.chopsticks.util.DbUtils._
import dev.chopsticks.util.AkkaHttpProtobufSupport._
import dev.chopsticks.util.DbUtils
import org.xerial.snappy.{SnappyCodec, SnappyInputStream}
import scalaz.zio.blocking.Blocking
import scalaz.zio.clock.Clock
import scalaz.zio.{Task, TaskR, ZIO}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

object HttpDb {
  final case class RequestFailureException(message: String, statusCode: StatusCode)
      extends RuntimeException(message)
      with NoStackTrace

  def decompressorFlow(res: HttpResponse): Flow[ByteString, ByteString, NotUsed] = {
    res
      .header[`Content-Encoding`]
      .flatMap(_.encodings.headOption)
      .flatMap {
        case HttpEncodings.gzip =>
          Some(Gzip.decoderFlow)
        case HttpEncodings.deflate =>
          Some(Deflate.decoderFlow)
        case _ =>
          None
      }
      .getOrElse(Flow.apply)
  }

  def apply[DbDef <: DbDefinition](
    definition: DbDef,
    host: String,
    port: Int,
    keepAliveInterval: FiniteDuration,
    iterateFailureDelayIncrement: FiniteDuration,
    iterateFailureDelayResetAfter: FiniteDuration,
    iterateFailureMaxDelay: FiniteDuration
  )(implicit akkaEnv: AkkaEnv): HttpDb[DbDef] = {
    new HttpDb[DbDef](
      definition,
      host,
      port,
      keepAliveInterval,
      iterateFailureDelayIncrement,
      iterateFailureDelayResetAfter,
      iterateFailureMaxDelay
    )
  }

  type MaybeLastKey = Option[Array[Byte]]
  private val noLastKey = Option.empty[Array[Byte]]

  def apiVersionHeader(version: String): RawHeader = RawHeader(HttpDbServer.API_VERSION_HEADER_NAME, version)

  final case class IterateSourceAttempt[S](
    lastState: S,
    nanoTime: Long,
    lastFailure: Option[String] = None,
    delay: FiniteDuration = Duration.Zero
  )

  private val byteStringToDbItemValueBatchFlow = {
    Flow[ByteString].map { b =>
      DbItemValueBatch
        .parseFrom(CodedInputStream.newInstance(b.getByteBuffers))
    }
  }

  private val byteStringToDbTailItemValueBatchFlow = {
    Flow[ByteString].map { b =>
      DbTailItemValueBatch
        .parseFrom(CodedInputStream.newInstance(b.getByteBuffers))
    }
  }

  private val byteStringToDbItemBatchFlow = {
    Flow[ByteString].map { b =>
      DbItemBatch
        .parseFrom(CodedInputStream.newInstance(b.getByteBuffers))
    }
  }

  private val byteStringToDbTailItemBatchFlow = {
    Flow[ByteString].map { b =>
      DbTailItemBatch
        .parseFrom(CodedInputStream.newInstance(b.getByteBuffers))
    }
  }

  private def byteStringToDbIndexedTailItemBatchFlow(parallelism: Int)(implicit ec: ExecutionContext) = {
    Flow[ByteString].mapAsync(parallelism) { b =>
      Future {
        DbIndexedTailItemBatch
          .parseFrom(CodedInputStream.newInstance(b.getByteBuffers))
      }
    }
  }

  type RetryOutput[S] = Either[Status.Status, IterateSourceAttempt[S]]

  private def stopRetrying[S](s: Status.Status): RetryOutput[S] = Left(s)
  private def retryWithAttempt[S](a: IterateSourceAttempt[S]): RetryOutput[S] = Right(a)

  private val iterateSourceRetryOnNetworkFailureMatcher: PartialFunction[Throwable, String] = {
    case e: StreamTcpException => e.getMessage

    case e: PeerClosedConnectionException => e.getMessage

    case UpstreamCompletedBeforeExplicitCompletionSignalException =>
      "UpstreamCompletedBeforeExplicitCompletionSignalException"

    case e: Exception if e.getClass.getName.contains("UnexpectedConnectionClosureException") =>
      "UnexpectedConnectionClosureException"
  }

  def handlePossibleCompletion(text: String): Future[ByteString] = {
    text match {
      case HttpDbServer.RESPONSE_COMPLETED_TEXT =>
        Future.successful(ByteString.empty)
      case _ =>
        Future.failed(new RuntimeException(s"Expected binary message, got streamed text: $text"))
    }
  }

  private def decompressFlow(parallelism: Int)(implicit ec: ExecutionContext): Flow[ByteString, ByteString, NotUsed] = {
    Flow[ByteString]
      .mapAsync(parallelism) { bs =>
        if (bs.length > SnappyCodec.MAGIC_LEN && SnappyCodec.hasMagicHeaderPrefix(
            bs.take(SnappyCodec.MAGIC_LEN).toArray
          )) {
          Future {
            val is = new SnappyInputStream(bs.iterator.asInputStream)
            val buffer = new Array[Byte](is.available())
            val builder = ByteString.newBuilder

            var len = 0

            do {
              len = is.read(buffer)
              if (len > 0) {
                builder.putBytes(buffer, 0, len)
              }
            } while (len >= 0)

            is.close()
            builder.result()
          }
        }
        else Future.successful(bs)
      }
  }

  private def httpRequestTask[Req, Res](
    request: HttpRequest,
    body: Req
  )(
    implicit sys: ActorSystem,
    mat: Materializer,
    mar: ToEntityMarshaller[Req],
    unmar: FromEntityUnmarshaller[Res]
  ): Task[Res] = {
    Task.fromFuture { implicit ec =>
      for {
        entity <- Marshal(body).to[RequestEntity]
        res <- Http()
          .singleRequest(
            request.copy(entity = entity)
          )
        bytes <- if (res.status.isSuccess()) {
          Unmarshal(res.entity.transformDataBytes(decompressorFlow(res))).to[Res]
        }
        else {
          res.entity.dataBytes
            .runWith(Sink.ignore)
            .flatMap { _ =>
              Future.failed(
                RequestFailureException(
                  s"Request failed with status code: ${res.status.value}. Request: $request",
                  res.status
                )
              )
            }
        }
      } yield bytes
    }
  }

}

final class HttpDb[DbDef <: DbDefinition](
  val definition: DbDef,
  serverHost: String,
  serverPort: Int,
  keepAliveInterval: FiniteDuration,
  iterateFailureDelayIncrement: FiniteDuration,
  iterateFailureDelayResetAfter: FiniteDuration,
  iterateFailureMaxDelay: FiniteDuration
)(implicit akkaEnv: AkkaEnv)
    extends DbInterface[DbDef]
    with StrictLogging {

  import HttpDb._
  import akkaEnv._

  val isLocal: Boolean = false

  def uriWithPath(path: String, scheme: String = "http", queryString: Option[String] = None): Uri = {
    Uri.apply(
      scheme = scheme,
      authority = Uri.Authority(Uri.Host(serverHost), serverPort),
      path = Uri.Path("/" + path),
      queryString = queryString
    )
  }

  private def decodeKeyForDisplay[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte]): String = {
    if (key.isEmpty) {
      "[EmptyKey]"
    }
    else {
      column.decodeKey(key) match {
        case Right(k) => k.toString
        case Left(e) => s"[KeyDecodeFailure ${e.toString}]"
      }
    }
  }

  def getTask[Col <: DbDef#BaseCol[_, _]](column: Col, constraints: DbKeyConstraintList): Task[Option[DbPair]] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.GET_PATH),
      headers = Vector(apiVersionHeader("20180428"))
    )

    val requestBody = DbGetRequest(
      columnId = column.id,
      constraints = constraints
    )

    httpRequestTask[DbGetRequest, DbGetResult](request, requestBody)
      .map { result =>
        result.item.map(v => (v.key.toByteArray, v.value.toByteArray))
      }
  }

  def batchGetTask[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    requests: Seq[DbKeyConstraintList]
  ): Task[Seq[Option[DbPair]]] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.BATCH_GET_PATH),
      headers = Vector(apiVersionHeader("20180428"))
    )

    val requestBody = DbBatchGetRequest(
      columnId = column.id,
      requests = requests
    )

    httpRequestTask[DbBatchGetRequest, DbBatchGetResult](request, requestBody)
      .map {
        case DbBatchGetResult(items) =>
          items.map(_.item.map(v => (v.key.toByteArray, v.value.toByteArray)))
      }
  }

  def statsTask: Task[Map[String, Double]] = {
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = uriWithPath(HttpDbServer.STATS_PATH),
      headers = Vector(apiVersionHeader("20180130"))
    )

    httpRequestTask[Unit, DbStats](request, ())
      .map { case DbStats(stats) => stats }
  }

  def estimateCount[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Long] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.ESTIMATE_COUNT_PATH),
      headers = Vector(apiVersionHeader("20170807.1"))
    )

    val requestBody = DbEstimateCountRequest(
      columnId = column.id
    )

    httpRequestTask[DbEstimateCountRequest, DbEstimateCountResult](request, requestBody)
      .map { case DbEstimateCountResult(count) => count }
  }

  def putTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.PUT_PATH),
      headers = Vector(apiVersionHeader("20171007"))
    )

    val requestBody = DbPutRequest(
      columnId = column.id,
      key = ProtoByteString.copyFrom(key),
      value = ProtoByteString.copyFrom(value)
    )

    httpRequestTask[DbPutRequest, DbPutResult](request, requestBody)
      .map(_ => ())
  }

  def deleteTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte]): Task[Unit] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.DELETE_PATH),
      headers = Vector(apiVersionHeader("20171106"))
    )

    val requestBody = DbDeleteRequest(
      columnId = column.id,
      key = ProtoByteString.copyFrom(key)
    )

    httpRequestTask[DbDeleteRequest, DbDeleteResult](request, requestBody)
      .map(_ => ())
  }

  def deletePrefixTask[Col <: DbDef#BaseCol[_, _]](column: Col, prefix: Array[Byte]): Task[Long] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.DELETE_PREFIX_PATH),
      headers = Vector(apiVersionHeader("20171115"))
    )

    val requestBody = DbDeletePrefixRequest(
      columnId = column.id,
      prefix = ProtoByteString.copyFrom(prefix)
    )

    httpRequestTask[DbDeletePrefixRequest, DbDeletePrefixResult](request, requestBody)
      .map(_.count)
  }

  /*def batchPutTask(items: Seq[(DbDef#BaseCol, Array[Byte], Array[Byte])])(implicit mat: Materializer): Task[Int] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.BATCH_PUT_PATH),
      headers = Vector(apiVersionHeader("20171007"))
    )

    val requestBody = DbBatchPutRequest(
      requests = items.map {
        case (column, key, value) =>
          DbPutRequest(
            columnId = column.id,
            key = ProtoByteString.copyFrom(key),
            value = ProtoByteString.copyFrom(value)
          )
      }.toArray
    )

    NetUtils
      .httpRequestTask[DbBatchPutRequest, DbBatchPutResult](request, requestBody)
      .map(_.count)
  }*/

  def iterateWithRequest(
    request: ByteString,
    path: String,
    apiVersion: String,
    decompressParallelism: Int = 1
  ): Source[ByteString, Future[Done]] = {

    val webSocketFlow = Http().webSocketClientFlow(
      WebSocketRequest(
        uri = uriWithPath(path, "ws"),
        extraHeaders = List(apiVersionHeader(apiVersion))
      )
    )

    Source
      .single(BinaryMessage.Strict(request))
      .concatMat(Source.maybe[Message])(Keep.right)
      .keepAlive(keepAliveInterval, () => DbWebsocketServerRequestHandlerFlow.KEEP_ALIVE)
      .viaMat(webSocketFlow)(Keep.both)
      .mapAsync(1) {
        case BinaryMessage.Strict(byteString) =>
          if (byteString.isEmpty) Future.failed(new RuntimeException("Got empty ByteString"))
          else Future.successful(byteString)

        case BinaryMessage.Streamed(s) =>
          s.runReduce(_ ++ _)

        case TextMessage.Strict(text) => handlePossibleCompletion(text)

        case TextMessage.Streamed(source) =>
          source
            .runReduce(_ ++ _)
            .flatMap(handlePossibleCompletion)

        case m => Future.failed(new RuntimeException(s"Expected binary message, got: $m"))
      }
      .via(Flow.fromGraph(new DbWebSocketClientResponseByteStringHandler))
      .via(decompressFlow(decompressParallelism))
      .watchTermination() {
        case ((p, wsf), f) =>
          wsf
            .flatMap {
              case _: ValidUpgrade =>
                Future.successful(Done)

              case InvalidUpgradeResponse(response, cause) =>
                val reason =
                  response.headers.find(_.is(VALIDATION_REJECTION_REASON_HEADER_NAME.toLowerCase())).map(_.value())
                val message = s"$cause. Reason: $reason"
                logger.error(message)
                Future.failed(new IllegalArgumentException(message))
            }
            .flatMap(_ => f)
            .andThen {
              case _ =>
                p.tryComplete(Success(None))
            }
      }
  }

  private def iterateSourceWithAutoRecovery[B, S](
    initialState: => S,
    createRequest: IterateSourceAttempt[S] => (
      Option[String],
      Source[ByteString, Future[Done]]
    ),
    parseBatchFlow: Flow[ByteString, B, NotUsed],
    lastStateFlow: Flow[B, B, Future[(S, Try[Done])]]
  )(retryMatcher: (S, S) => PartialFunction[Try[Done], RetryOutput[S]]): Source[B, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {

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

        enqueue(IterateSourceAttempt[S](initialState, System.nanoTime())).map {
          _ =>
            attemptSource
              .via(AkkaStreamUtils.statefulMapOptionFlow(() => {
                var lastAttempt: Option[IterateSourceAttempt[S]] = None

                attempt => {
                  lastAttempt = lastAttempt
                    .map { last =>
                      val elapse = Duration.fromNanos(Math.abs(attempt.nanoTime - (last.nanoTime + last.delay.toNanos)))
                      if (elapse <= iterateFailureDelayResetAfter) {
                        val delay = last.delay + iterateFailureDelayIncrement
                        attempt.copy(delay = if (delay <= iterateFailureMaxDelay) delay else iterateFailureMaxDelay)
                      }
                      else attempt
                    }
                    .orElse(Some(attempt))

                  lastAttempt
                }
              }))
              .mapAsync(1) { attempt =>
                val fa = Future.successful(attempt)
                if (attempt.delay > Duration.Zero) akka.pattern.after(attempt.delay, actorSystem.scheduler)(fa)
                else fa
              }
              .flatMapConcat {
                attempt =>
                  val (maybeLog, source) = createRequest(attempt)

                  maybeLog.foreach(m => logger.warn(m))

                  source
                    .via(parseBatchFlow)
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

  private val iterateValuesBatchSourceLastStateFlow = {
    Flow.fromGraph(
      LastStateFlow[DbItemValueBatch, Option[DbItemValueBatch], MaybeLastKey](
        seed = Option.empty[DbItemValueBatch],
        next = (_, b) => Some(b),
        result = _.map(_.lastKey.toByteArray)
      )
    )
  }

  private def iterateValuesBatchSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbItemValueBatch, Future[NotUsed]] = {
    def createRequest(attempt: IterateSourceAttempt[MaybeLastKey]) = {
      val maybeLastKey = attempt.lastState
      val lastFailure = attempt.lastFailure
      val lastKey = maybeLastKey.getOrElse(Array.emptyByteArray)
      val lastKeyDisplay = decodeKeyForDisplay(column, lastKey)

      val newRange = DbUtils.resumeRange(range, lastKey, lastKeyDisplay)

      val attemptInfo = lastFailure.map { f =>
        s"Retrying was-delayed=${attempt.delay} call=iterateValuesSource column=${column.id} range=${newRange.show} " +
          s"lastKey=$lastKeyDisplay " +
          s"lastFailure=$f"
      }

      val request = DbIterateValuesRequest(
        columnId = column.id,
        range = newRange,
        maxBatchSize = clientOptions.maxBatchBytes
      )

      (attemptInfo, iterateWithRequest(ByteString(request.toByteArray), HttpDbServer.ITERATE_VALUES_PATH, "20181013"))
    }

    iterateSourceWithAutoRecovery[DbItemValueBatch, MaybeLastKey](
      noLastKey,
      createRequest,
      byteStringToDbItemValueBatchFlow,
      iterateValuesBatchSourceLastStateFlow
    ) { (priorState, state) =>
      {
        case Success(v) =>
          stopRetrying(Status.Success(v))

        case Failure(e) =>
          iterateSourceRetryOnNetworkFailureMatcher
            .andThen { s =>
              retryWithAttempt(IterateSourceAttempt(state.orElse(priorState), System.nanoTime(), Some(s)))
            }
            .applyOrElse(e, (ex: Throwable) => stopRetrying(Status.Failure(ex)))
      }
    }
  }

  def iterateValuesSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbValueBatch, Future[NotUsed]] = {
    iterateValuesBatchSource(column, range)
      .map(_.values.map(_.toByteArray))
  }

  private val iterateBatchSourceLastStateFlow = {
    Flow.fromGraph(
      LastStateFlow[DbItemBatch, Option[DbItemBatch], MaybeLastKey](
        seed = Option.empty[DbItemBatch],
        next = (_, b) => Some(b),
        result = _.map(_.items.last.key.toByteArray)
      )
    )
  }

  def iterateBatchSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbItemBatch, Future[NotUsed]] = {
    def createRequest(attempt: IterateSourceAttempt[MaybeLastKey]) = {
      val maybeLastKey = attempt.lastState
      val lastFailure = attempt.lastFailure
      val lastKey = maybeLastKey.getOrElse(Array.emptyByteArray)
      val lastKeyDisplay = decodeKeyForDisplay(column, lastKey)
      val newRange = DbUtils.resumeRange(range, lastKey, lastKeyDisplay)

      val attemptInfo = lastFailure.map { f =>
        s"Retrying was-delayed=${attempt.delay} call=iterateSource column=${column.id} range=${newRange.show} " +
          s"lastKey=$lastKeyDisplay lastFailure=$f"
      }

      val request = DbIterateRequest(
        columnId = column.id,
        range = newRange,
        maxBatchSize = clientOptions.maxBatchBytes
      )

      (attemptInfo, iterateWithRequest(ByteString(request.toByteArray), HttpDbServer.ITERATE_PATH, "20181013"))
    }

    iterateSourceWithAutoRecovery[DbItemBatch, MaybeLastKey](
      noLastKey,
      createRequest,
      byteStringToDbItemBatchFlow,
      iterateBatchSourceLastStateFlow
    ) { (priorState, lastState) =>
      {
        case Success(v) => stopRetrying(Status.Success(v))
        case Failure(e) =>
          iterateSourceRetryOnNetworkFailureMatcher
            .andThen { s =>
              retryWithAttempt(
                IterateSourceAttempt(lastState.orElse(priorState), System.nanoTime(), Some(s))
              )
            }
            .applyOrElse(e, (ex: Throwable) => stopRetrying(Status.Failure(ex)))

      }
    }
  }

  def iterateSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbBatch, Future[NotUsed]] = {
    iterateBatchSource(column, range)
      .map(
        batch =>
          batch.items.map { item =>
            (item.key.toByteArray, item.value.toByteArray)
          }
      )
  }

  def openTask(): Task[Unit] = {
    val uuid = UUID.randomUUID().toString

    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = uriWithPath(HttpDbServer.PING_PATH, queryString = Some(s"ping=$uuid")),
      headers = List(apiVersionHeader("20170807.1"))
    )

    httpRequestTask[Unit, DbPingResponse](request, ())
      .map { case DbPingResponse(pong) => assert(pong == uuid) }
  }

  def closeTask(): TaskR[Blocking with Clock, Unit] = ZIO.unit

  type TailLastState = Option[Either[Exception, MaybeLastKey]]

  private def retryWithLastState[S](state: S, lastFailure: Option[String]) = {
    retryWithAttempt[S](
      IterateSourceAttempt[S](
        state,
        System.nanoTime(),
        lastFailure
      )
    )
  }

  private def retryWithLastKey(maybeLastKey: MaybeLastKey, lastFailure: Option[String]) = {
    retryWithLastState(maybeLastKey, lastFailure)
  }

  private val tailBatchSourceLastStateFlow = {
    Flow.fromGraph(
      LastStateFlow[DbTailItemBatch, Option[DbTailItemBatch], MaybeLastKey](
        Option.empty[DbTailItemBatch],
        next = (_, b) => Some(b),
        result = _.flatMap { b =>
          b.value match {
            case DbTailItemBatch.Value.Empty =>
              throw new IllegalStateException("Received DbTailItemBatch.Value.Empty")

            case DbTailItemBatch.Value.Batch(value) =>
              Some(value.items.last.key.toByteArray)

            case DbTailItemBatch.Value.EmptyTail(value) =>
              value.lastKey.map(_.toByteArray)
          }
        }
      )
    )
  }

  private def tailBatchSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbTailItemBatch, Future[NotUsed]] = {
    def createRequest(attempt: IterateSourceAttempt[MaybeLastKey]) = {
      val maybeLastKey = attempt.lastState
      val lastFailure = attempt.lastFailure
      val lastKey = maybeLastKey.getOrElse(Array.emptyByteArray)
      val lastKeyDisplay = decodeKeyForDisplay(column, lastKey)

      val newRange = DbUtils.resumeRange(range, lastKey, lastKeyDisplay)

      val attemptInfo = lastFailure.map { f =>
        s"Retrying was-delayed=${attempt.delay} call=tailSource column=${column.id} range=${newRange.show} " +
          s"pollingInterval=${clientOptions.tailPollingInterval} " +
          s"lastKey=$lastKeyDisplay " +
          s"lastFailure=$f"
      }

      val request = DbTailRequest(
        columnId = column.id,
        range = newRange,
        pollingInterval = clientOptions.tailPollingInterval,
        maxBatchSize = clientOptions.maxBatchBytes
      )

      (attemptInfo, iterateWithRequest(ByteString(request.toByteArray), HttpDbServer.TAIL_PATH, "20181013"))
    }

    iterateSourceWithAutoRecovery[DbTailItemBatch, MaybeLastKey](
      noLastKey,
      createRequest,
      byteStringToDbTailItemBatchFlow,
      tailBatchSourceLastStateFlow
    ) { (priorState, lastState) =>
      {
        case Success(_) =>
          retryWithLastKey(lastState.orElse(priorState), Some("Upstream completed but currently in tail mode"))

        case Failure(e: DbException) => stopRetrying(Status.Failure(e))

        case Failure(e) =>
          retryWithLastKey(lastState.orElse(priorState), Some(e.toString))
      }
    }
  }

  def tailSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbTailBatch, Future[NotUsed]] = {
    tailBatchSource(column, range)
      .map { batch: DbTailItemBatch =>
        batch.value match {
          case DbTailItemBatch.Value.Empty => throw new IllegalArgumentException("Got empty value for DbTailItemBatch")
          case DbTailItemBatch.Value.Batch(value) =>
            Right(value.items.map { item =>
              (item.key.toByteArray, item.value.toByteArray)
            })
          case DbTailItemBatch.Value.EmptyTail(value) =>
            Left(EmptyTail(value.time, value.lastKey.map(_.toByteArray)))
        }
      }
  }

  private val tailValueBatchSourceLastStateFlow = {
    Flow.fromGraph(
      LastStateFlow[DbTailItemValueBatch, Option[DbTailItemValueBatch], MaybeLastKey](
        Option.empty[DbTailItemValueBatch],
        next = (_, b) => Some(b),
        result = _.flatMap { b =>
          b.value match {
            case DbTailItemValueBatch.Value.Empty =>
              throw new IllegalStateException("Received DbTailItemBatch.Value.Empty")

            case DbTailItemValueBatch.Value.Batch(value) =>
              Some(value.lastKey.toByteArray)

            case DbTailItemValueBatch.Value.EmptyTail(value) =>
              value.lastKey.map(_.toByteArray)
          }
        }
      )
    )
  }

  private val batchTailSourceLastStateFlow = {
    Flow.fromGraph(
      LastStateFlow[DbIndexedTailItemBatch, mutable.Map[Int, ProtoByteString], Map[Int, Array[Byte]]](
        seed = mutable.Map.empty,
        next = (m, b) => {
          val index = b.index
          b.batch.value match {
            case DbTailItemBatch.Value.Empty =>
              throw new IllegalStateException("Received DbTailItemBatch.Value.Empty")

            case DbTailItemBatch.Value.Batch(value) =>
              m += (index -> value.items.last.key)

            case DbTailItemBatch.Value.EmptyTail(value) =>
              value.lastKey match {
                case Some(k) =>
                  m += (index -> k)
                case None =>
                  m
              }
          }
        },
        result = _.mapValues(_.toByteArray).toMap
      )
    )
  }

  private def batchTailBatchSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    ranges: List[DbKeyRange]
  )(
    implicit clientOptions: DbClientOptions
  ): Source[DbIndexedTailItemBatch, Future[NotUsed]] = {
    type LastKeyMap = Map[Int, Array[Byte]]

    val parallelism = Math.max(1, Math.min(ranges.size, Runtime.getRuntime.availableProcessors()))

    def createRequest(attempt: IterateSourceAttempt[LastKeyMap]) = {
      val lastKeyMap = attempt.lastState
      val lastFailure = attempt.lastFailure
      val lastKeys = ranges.indices.map { index =>
        lastKeyMap.getOrElse(index, Array.emptyByteArray)
      }
      val newRanges = ranges.zip(lastKeys).map {
        case (range, lastKey) =>
          DbUtils.resumeRange(range, lastKey, decodeKeyForDisplay(column, lastKey))
      }
      val sampleSize = 2
      val moreText = if (newRanges.size > sampleSize) s" (+${newRanges.size - sampleSize} more)" else ""

      val attemptInfo = lastFailure.map { f =>
        s"Retrying was-delayed=${attempt.delay} call=batchTailSource column=${column.id} " +
          s"ranges=${newRanges.take(sampleSize).map(_.show).mkString(",")}$moreText " +
          s"pollingInterval=${clientOptions.tailPollingInterval} " +
          s"lastKeys=${lastKeys.take(sampleSize).map(k => decodeKeyForDisplay(column, k)).mkString(",")}$moreText " +
          s"lastFailure=$f"
      }

      val request = DbBatchTailRequest(
        columnId = column.id,
        ranges = newRanges,
        pollingInterval = clientOptions.tailPollingInterval,
        maxBatchSize = clientOptions.maxBatchBytes
      )

      (
        attemptInfo,
        iterateWithRequest(
          request = ByteString(request.toByteArray),
          path = HttpDbServer.BATCH_TAIL_PATH,
          apiVersion = "20190212",
          decompressParallelism = parallelism
        )
      )
    }

    iterateSourceWithAutoRecovery[DbIndexedTailItemBatch, LastKeyMap](
      Map.empty,
      createRequest,
      byteStringToDbIndexedTailItemBatchFlow(parallelism)(actorSystem.dispatcher),
      batchTailSourceLastStateFlow
    ) { (priorState, lastState) =>
      {
        case Success(_) =>
          retryWithLastState(priorState ++ lastState, Some("Upstream completed but currently in tail mode"))

        case Failure(e: DbException) => stopRetrying(Status.Failure(e))

        case Failure(e) =>
          retryWithLastState(priorState ++ lastState, Some(e.toString))
      }
    }
  }

  def batchTailSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    ranges: List[DbKeyRange]
  )(implicit clientOptions: DbClientOptions): Source[(Int, DbTailBatch), Future[NotUsed]] = {
    val parallelism = Math.max(1, Math.min(ranges.size, Runtime.getRuntime.availableProcessors()))

    batchTailBatchSource(column, ranges)
      .mapAsync(parallelism) {
        case DbIndexedTailItemBatch(index, batch) =>
          batch.value match {
            case DbTailItemBatch.Value.Empty =>
              throw new IllegalArgumentException("Got empty value for DbTailItemBatch")
            case DbTailItemBatch.Value.Batch(value) =>
              Future {
                (index, Right(value.items.map { item =>
                  (item.key.toByteArray, item.value.toByteArray)
                }))
              }(actorSystem.dispatcher)
            case DbTailItemBatch.Value.EmptyTail(value) =>
              Future.successful {
                (index, Left(EmptyTail(value.time, value.lastKey.map(_.toByteArray))))
              }
          }
      }
  }

  def tailValuesBatchSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbTailItemValueBatch, Future[NotUsed]] = {
    def createRequest(attempt: IterateSourceAttempt[MaybeLastKey]) = {
      val maybeLastKey = attempt.lastState
      val lastFailure = attempt.lastFailure
      val lastKey = maybeLastKey.getOrElse(Array.emptyByteArray)
      val lastKeyDisplay = decodeKeyForDisplay(column, lastKey)
      val newRange = DbUtils.resumeRange(range, lastKey, lastKeyDisplay)

      val attemptInfo = lastFailure.map { f =>
        s"Retrying was-delayed=${attempt.delay} call=tailValuesSource column=${column.id} range=${newRange.show} " +
          s"pollingInterval=${clientOptions.tailPollingInterval} " +
          s"lastKey=$lastKeyDisplay " +
          s"lastFailure=$f"
      }

      val request = DbTailValuesRequest(
        columnId = column.id,
        range = newRange,
        pollingInterval = clientOptions.tailPollingInterval,
        maxBatchSize = clientOptions.maxBatchBytes
      )

      (attemptInfo, iterateWithRequest(ByteString(request.toByteArray), HttpDbServer.TAIL_VALUES_PATH, "20181013"))
    }

    iterateSourceWithAutoRecovery[DbTailItemValueBatch, MaybeLastKey](
      noLastKey,
      createRequest,
      byteStringToDbTailItemValueBatchFlow,
      tailValueBatchSourceLastStateFlow
    ) { (priorState, lastState) =>
      {
        case Success(_) =>
          retryWithLastKey(lastState.orElse(priorState), Some("Upstream completed but currently in tail mode"))

        case Failure(e: DbException) => stopRetrying(Status.Failure(e))

        case Failure(e) =>
          retryWithLastKey(lastState.orElse(priorState), Some(e.toString))
      }
    }
  }

  def tailValuesSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbTailValueBatch, Future[NotUsed]] = {
    tailValuesBatchSource(column, range)
      .map { batch =>
        batch.value match {
          case Value.Empty => throw new IllegalArgumentException("Got empty value for DbTailItemBatch")
          case Value.Batch(value) =>
            Right(value.values.map(_.toByteArray))
          case Value.EmptyTail(value) =>
            Left(EmptyTail(value.time, value.lastKey.map(_.toByteArray)))
        }
      }
  }

  def compactTask(): Task[Unit] =
    Task.fail(UnsupportedDbOperationException("Triggering compaction from a remote client is not supported for now"))

  def transactionTask(actions: Seq[DbTransactionAction]): Task[Unit] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.TRANSACTION_PATH),
      headers = List(apiVersionHeader("20171106"))
    )

    val requestBody = DbTransactionRequest(
      actions = actions
    )

    httpRequestTask[DbTransactionRequest, DbTransactionResult](request, requestBody)
      .map(_ => ())
  }

  def startBulkInsertsTask(): Task[Unit] = ???

  def endBulkInsertsTask(): Task[Unit] = ???

  def dropColumnFamily[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Unit] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = uriWithPath(HttpDbServer.DROP_COLUMN_FAMILY_PATH),
      headers = List(apiVersionHeader("20190212"))
    )

    httpRequestTask[DbDropColumnFamilyRequest, DbDropColumnFamilyResponse](
      request,
      DbDropColumnFamilyRequest(column.id)
    ).map { case DbDropColumnFamilyResponse(id) => assert(id == column.id) }
  }
}
