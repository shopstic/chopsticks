package dev.chopsticks.kvdb

import java.io.ByteArrayOutputStream

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.DbInterface._
import dev.chopsticks.kvdb.DbWebsocketServerRequestHandlerFlow.ClientTerminatedPrematurelyException
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.proto.db._
import dev.chopsticks.kvdb.util.DbUtils.Implicits.defaultDbClientOptions
import dev.chopsticks.kvdb.util.DbUtils._
import kamon.Kamon
import kamon.metric.{CounterMetric, MeasurementUnit}
import org.xerial.snappy.SnappyOutputStream
import scalaz.zio.Task

import scala.concurrent.Future

object HttpDbServer extends StrictLogging {
  val API_VERSION_HEADER_NAME = "X-Api-Version"
  val VALIDATION_REJECTION_REASON_HEADER_NAME = "Validation-Rejection-Reason"

  val RESPONSE_COMPLETED_TEXT = "completed"

  val RESPONSE_COMPLETED: Message = TextMessage.Strict(RESPONSE_COMPLETED_TEXT)

  val STATS_PATH = "stats"
  val PING_PATH = "ping"
  val TAIL_PATH = "tail"
  val BATCH_TAIL_PATH = "batch-tail"
  val TAIL_VALUES_PATH = "tail-values"
  val ITERATE_PATH = "iterate"
  val ITERATE_VALUES_PATH = "iterate-values"
  val ESTIMATE_COUNT_PATH = "estimate-count"

  val GET_PATH = "get"
  val BATCH_GET_PATH = "batch-get"

  val PUT_PATH = "put"
  val BATCH_PUT_PATH = "batch-put"

  val DELETE_PATH = "delete"
  val DELETE_PREFIX_PATH = "delete-prefix"

  val TRANSACTION_PATH = "transaction"
  val DROP_COLUMN_FAMILY_PATH = "drop-column-family"

  private val MIN_COMPRESSION_BLOCK_SIZE = 8 * 1024

  trait HttpDbServerMetrics {
    protected val dbServerPrefix: String = "db_"

    lazy val dbByteCounter: CounterMetric =
      Kamon.counter(dbServerPrefix + "uncompressed_bytes", MeasurementUnit.information.bytes)
//
    lazy val dbCompressedByteCounter: CounterMetric =
      Kamon.counter(dbServerPrefix + "compressed_bytes", MeasurementUnit.information.bytes)
//
//    lazy val dbWrappedCompressedByteCounter: Counter =
//      Counter.build().name(dbServerPrefix + "wrapped_compressed_bytes").help("Wrapped Compressed Bytes").register()

    lazy val dbBatchCounter: CounterMetric = Kamon.counter(dbServerPrefix + "batches")
  }

  private val exceptionHandler: Flow[Array[Byte], Message, NotUsed] = Flow[Array[Byte]]
    .prefixAndTail(1)
    .flatMapConcat {
      case (first, rest) =>
        if (first.nonEmpty) {
          Source
            .single(
              DbOperationPossibleException(
                DbOperationPossibleException.Content.Message(
                  ProtoByteString.copyFrom(first.head)
                )
              ).toByteArray
            ) ++ rest
        }
        else rest
    }
    .map(b => BinaryMessage.Strict(ByteString.fromArrayUnsafe(b)))
    .recover {
      case ex: DbException =>
        val exceptionType = ex.exceptionType
        val message = ex.getMessage
        BinaryMessage.Strict(
          ByteString.fromArrayUnsafe(
            DbOperationPossibleException(
              DbOperationPossibleException.Content.Exception(
                DbOperationException(exceptionType, message)
              )
            ).toByteArray
          )
        )
    }

  private[kvdb] def createHandler(handler: Message => Source[Array[Byte], NotUsed]): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .via(Flow.fromGraph(new DbWebsocketServerRequestHandlerFlow))
      .flatMapConcat { request =>
        handler(request).via(exceptionHandler) ++ Source.single(RESPONSE_COMPLETED)
      }
      .takeWhile(_ ne RESPONSE_COMPLETED, inclusive = true)
      .recoverWithRetries(1, {
        case ClientTerminatedPrematurelyException =>
          logger.debug("ClientTerminatedPrematurelyException")
          Source.empty
      })
  }

  def compress(bytes: Array[Byte]): Array[Byte] = {
    if (bytes.length >= MIN_COMPRESSION_BLOCK_SIZE) {
      val out = new ByteArrayOutputStream(bytes.length)
      val os = new SnappyOutputStream(out)
      os.write(bytes)
      os.close()
      out.toByteArray
    }
    else bytes
  }

  def encodeAsDbItemBatch(batch: DbBatch): DbItemBatch = {
    DbItemBatch(batch.map(p => DbItem(ProtoByteString.copyFrom(p._1), ProtoByteString.copyFrom(p._2))))
  }

  def encodeAsDbItemValueBatch(batch: DbBatch): DbItemValueBatch = {
    DbItemValueBatch(ProtoByteString.copyFrom(batch.last._1), batch.map(p => ProtoByteString.copyFrom(p._2)))
  }

  def encodeAsDbTailItemBatch(tailBatch: DbTailBatch): DbTailItemBatch = tailBatch match {
    case Left(emptyTail) =>
      DbTailItemBatch(
        DbTailItemBatch.Value.EmptyTail(
          DbEmptyTail(
            time = emptyTail.time,
            lastKey = emptyTail.lastKey.map(k => ProtoByteString.copyFrom(k))
          )
        )
      )

    case Right(batch) =>
      DbTailItemBatch(
        DbTailItemBatch.Value.Batch(
          DbItemBatch(batch.map(p => DbItem(ProtoByteString.copyFrom(p._1), ProtoByteString.copyFrom(p._2))))
        )
      )
  }

  def encodeAsDbIndexedTailItemBatch(indexedTailBatch: DbIndexedTailBatch): DbIndexedTailItemBatch =
    indexedTailBatch match {
      case (index, batch) =>
        DbIndexedTailItemBatch(index = index, batch = encodeAsDbTailItemBatch(batch))
    }

  def encodeAsDbTailItemValueBatch(tailBatch: DbTailBatch): DbTailItemValueBatch = tailBatch match {
    case Left(emptyTail) =>
      DbTailItemValueBatch(
        DbTailItemValueBatch.Value.EmptyTail(
          DbEmptyTail(
            time = emptyTail.time,
            lastKey = emptyTail.lastKey.map(k => ProtoByteString.copyFrom(k))
          )
        )
      )

    case Right(batch) =>
      DbTailItemValueBatch(
        DbTailItemValueBatch.Value.Batch(
          DbItemValueBatch(ProtoByteString.copyFrom(batch.last._1), batch.map(p => ProtoByteString.copyFrom(p._2)))
        )
      )
  }

  def apply[DbDef <: DbDefinition](
    db: DbInterface[DbDef],
    metrics: Option[HttpDbServer.HttpDbServerMetrics] = None
  )(implicit akkaEnv: AkkaEnv): HttpDbServer[DbDef] = {
    new HttpDbServer(db, metrics)
  }
}

final class HttpDbServer[DbDef <: DbDefinition](
  db: DbInterface[DbDef],
  metrics: Option[HttpDbServer.HttpDbServerMetrics] = None
)(implicit akkaEnv: AkkaEnv) {

  type Col = DbDef#BaseCol[_, _]
  import HttpDbServer._
  import akkaEnv._

  private def invalidDbColumnFamilyException(columnId: String): InvalidDbColumnFamilyException = {
    InvalidDbColumnFamilyException(
      s"Column family with id '$columnId' doesn't exist in this database '${db.definition.getClass.getName}'"
    )
  }

  private val measureUncompressedSink = metrics
    .map { m =>
      Sink.foreach[Array[Byte]] { b =>
        m.dbByteCounter.increment(b.length.toLong)
        m.dbBatchCounter.increment()
      }
    }
    .getOrElse(Sink.ignore)

  private val measureCompressedSink = metrics
    .map { m =>
      Sink.foreach[Array[Byte]] { b =>
        m.dbCompressedByteCounter.increment(b.length.toLong)
      }
    }
    .getOrElse(Sink.ignore)

  private def serializationFlow(parallelism: Int = 1) =
    Flow[scalapb.GeneratedMessage]
      .mapAsync(parallelism)(b => Future(b.toByteArray))
      .wireTap(measureUncompressedSink)
      .mapAsync(parallelism)(b => Future(compress(b)))
      .wireTap(measureCompressedSink)

  private val iterateFlow: PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    case BinaryMessage.Strict(byteString) =>
      val request = DbIterateRequest.parseFrom(byteString.toArray)

      val columnId = request.columnId

      val source = db
        .columnFamilyWithId(columnId)
        .map { col =>
          db.iterateSource(col, request.range)(defaultDbClientOptions.copy(maxBatchBytes = request.maxBatchSize))
            .mapMaterializedValue(_ => NotUsed)
        }
        .getOrElse(Source.failed[DbBatch](invalidDbColumnFamilyException(columnId)))
        .map(encodeAsDbItemBatch)
        .via(serializationFlow())

      source

    case m =>
      Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
  }

  private val iterateValuesFlow: PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    case BinaryMessage.Strict(byteString) =>
      val request = DbIterateValuesRequest.parseFrom(byteString.toArray)

      val columnId = request.columnId

      val source = db
        .columnFamilyWithId(columnId)
        .map { col =>
          db.iterateSource(col, request.range)(defaultDbClientOptions.copy(maxBatchBytes = request.maxBatchSize))
            .mapMaterializedValue(_ => NotUsed)
        }
        .getOrElse(Source.failed[DbBatch](invalidDbColumnFamilyException(columnId)))
        .map(encodeAsDbItemValueBatch)
        .via(serializationFlow())

      source

    case m =>
      Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
  }

  private val tailFlow: PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    case BinaryMessage.Strict(byteString) =>
      val request = DbTailRequest.parseFrom(byteString.toArray)

      val columnId = request.columnId

      val source = db
        .columnFamilyWithId(columnId)
        .map { col =>
          db.tailSource(col, request.range)(
              defaultDbClientOptions
                .copy(maxBatchBytes = request.maxBatchSize, tailPollingInterval = request.pollingInterval)
            )
            .mapMaterializedValue(_ => NotUsed)
        }
        .getOrElse(Source.failed[DbTailBatch](invalidDbColumnFamilyException(columnId)))
        .map(encodeAsDbTailItemBatch)
        .via(serializationFlow())

      source

    case m =>
      Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
  }

  private val batchTailFlow: PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    case BinaryMessage.Strict(byteString) =>
      val request = DbBatchTailRequest.parseFrom(byteString.toArray)

      val columnId = request.columnId
      val ranges = request.ranges
      val parallelism = Math.max(1, Math.min(ranges.size, Runtime.getRuntime.availableProcessors()))

      val source = db
        .columnFamilyWithId(columnId)
        .map { col =>
          db.batchTailSource(col, ranges)(
              defaultDbClientOptions
                .copy(maxBatchBytes = request.maxBatchSize, tailPollingInterval = request.pollingInterval)
            )
            .mapMaterializedValue(_ => NotUsed)
        }
        .getOrElse(Source.failed[DbIndexedTailBatch](invalidDbColumnFamilyException(columnId)))
        .mapAsync(parallelism)(b => Future(encodeAsDbIndexedTailItemBatch(b)))
        .via(serializationFlow(parallelism))

      source

    case m =>
      Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
  }

  private val tailValuesFlow: PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    case BinaryMessage.Strict(byteString) =>
      val request = DbTailValuesRequest.parseFrom(byteString.toArray)

      val columnId = request.columnId

      val source = db
        .columnFamilyWithId(columnId)
        .map { col =>
          db.tailSource(col, request.range)(
              defaultDbClientOptions
                .copy(maxBatchBytes = request.maxBatchSize, tailPollingInterval = request.pollingInterval)
            )
            .mapMaterializedValue(_ => NotUsed)
        }
        .getOrElse(Source.failed[DbTailBatch](invalidDbColumnFamilyException(columnId)))
        .map(encodeAsDbTailItemValueBatch)
        .via(serializationFlow())

      source

    case m =>
      Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
  }

  private def runTaskWithColumnId[V](columnId: String)(f: Col => Task[V]): Future[V] = {
    db.columnFamilyWithId(columnId)
      .map(f)
      .map(unsafeRunToFuture)
      .getOrElse(Future.failed(invalidDbColumnFamilyException(columnId)))
  }

  private def stats: Future[DbStats] = {
    unsafeRunToFuture(
      db.statsTask
        .map(DbStats.apply)
    )
  }

  private def estimateCount(request: DbEstimateCountRequest): Future[DbEstimateCountResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.estimateCount(column)
        .map { count =>
          DbEstimateCountResult(count)
        }
    }
  }

  private def getItem(request: DbGetRequest): Future[DbGetResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.getTask(column, request.constraints)
        .map { maybeItem =>
          DbGetResult(
            maybeItem.map(v => DbItem(key = ProtoByteString.copyFrom(v._1), value = ProtoByteString.copyFrom(v._2)))
          )
        }
    }
  }

  private def batchGetItems(request: DbBatchGetRequest): Future[DbBatchGetResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.batchGetTask(column, request.requests)
        .map { items =>
          DbBatchGetResult(items = items.map { maybeItem =>
            MaybeDbItem(maybeItem.map { v =>
              DbItem(key = ProtoByteString.copyFrom(v._1), value = ProtoByteString.copyFrom(v._2))
            })
          }.toArray)
        }
    }
  }

  private def putItem(request: DbPutRequest): Future[DbPutResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.putTask(column, request.key.toByteArray, request.value.toByteArray)
        .map(_ => DbPutResult.defaultInstance)
    }
  }

  private def deleteItem(request: DbDeleteRequest): Future[DbDeleteResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.deleteTask(column, request.key.toByteArray)
        .map(_ => DbDeleteResult.defaultInstance)
    }
  }

  private def deletePrefix(request: DbDeletePrefixRequest): Future[DbDeletePrefixResult] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.deletePrefixTask(column, request.prefix.toByteArray)
        .map(DbDeletePrefixResult.apply)
    }
  }

  private def transaction(request: DbTransactionRequest): Future[DbTransactionResult] = {
    unsafeRunToFuture(
      db.transactionTask(request.actions)
        .map(_ => DbTransactionResult.defaultInstance)
    )
  }

  private def dropColumnFamily(request: DbDropColumnFamilyRequest): Future[DbDropColumnFamilyResponse] = {
    runTaskWithColumnId(request.columnId) { column =>
      db.dropColumnFamily(column)
        .map(_ => DbDropColumnFamilyResponse(column.id))
    }
  }

  //noinspection ScalaStyle
  // scalastyle:off method.length
  private def route: Route = {
    // scalastyle:on method.length
    import Directives._
    import dev.chopsticks.kvdb.util.AkkaHttpProtobufSupport._

    def checkVersion(serverVersion: String): Directive0 = {
      headerValueByName(API_VERSION_HEADER_NAME).flatMap { clientVersion =>
        validate(
          clientVersion == serverVersion,
          s"Client version differs from server version ($clientVersion vs $serverVersion)"
        )
      }
    }

    val rejectionHandler = RejectionHandler
      .newBuilder()
      .handle {
        case ValidationRejection(msg, _) =>
          complete(
            HttpResponse(
              status = StatusCodes.BadRequest,
              headers = List(RawHeader(VALIDATION_REJECTION_REASON_HEADER_NAME, msg)),
              entity = HttpEntity(msg)
            )
          )
      }
      .result()

    /*
     * API versions below aren't duplicate code.
     * They are intentionally specified individually per endpoint so that each can evolve separately over time
     * */
    handleRejections(rejectionHandler) {
      concat(
        path(PING_PATH) {
          get {
            checkVersion("20170807.1") {
              parameter('ping) { ping =>
                complete(DbPingResponse(pong = ping))
              }
            }
          }
        },
        path(STATS_PATH) {
          get {
            checkVersion("20180130") {
              complete(stats)
            }
          }
        },
        path(ITERATE_PATH) {
          get {
            checkVersion("20181013") {
              handleWebSocketMessages(createHandler(iterateFlow))
            }
          }
        },
        path(ITERATE_VALUES_PATH) {
          get {
            checkVersion("20181013") {
              handleWebSocketMessages(createHandler(iterateValuesFlow))
            }
          }
        },
        path(TAIL_PATH) {
          get {
            checkVersion("20181013") {
              handleWebSocketMessages(createHandler(tailFlow))
            }
          }
        },
        path(BATCH_TAIL_PATH) {
          get {
            checkVersion("20190212") {
              handleWebSocketMessages(createHandler(batchTailFlow))
            }
          }
        },
        path(TAIL_VALUES_PATH) {
          get {
            checkVersion("20181013") {
              handleWebSocketMessages(createHandler(tailValuesFlow))
            }
          }
        },
        path(ESTIMATE_COUNT_PATH) {
          post {
            checkVersion("20170807.1") {
              entity(as[DbEstimateCountRequest]) { request =>
                complete(estimateCount(request))
              }
            }
          }
        },
        path(GET_PATH) {
          post {
            checkVersion("20180428") {
              entity(as[DbGetRequest]) { request =>
                complete(getItem(request))
              }
            }
          }
        },
        path(BATCH_GET_PATH) {
          post {
            checkVersion("20180428") {
              entity(as[DbBatchGetRequest]) { request =>
                complete(batchGetItems(request))
              }
            }
          }
        },
        path(PUT_PATH) {
          post {
            checkVersion("20171007") {
              entity(as[DbPutRequest]) { request =>
                complete(putItem(request))
              }
            }
          }
        },
        path(DELETE_PATH) {
          post {
            checkVersion("20171106") {
              entity(as[DbDeleteRequest]) { request =>
                complete(deleteItem(request))
              }
            }
          }
        },
        path(DELETE_PREFIX_PATH) {
          post {
            checkVersion("20171115") {
              entity(as[DbDeletePrefixRequest]) { request =>
                complete(deletePrefix(request))
              }
            }
          }
        },
        path(TRANSACTION_PATH) {
          post {
            checkVersion("20171106") {
              entity(as[DbTransactionRequest]) { request =>
                complete(transaction(request))
              }
            }
          }
        },
        path(DROP_COLUMN_FAMILY_PATH) {
          post {
            checkVersion("20190212") {
              entity(as[DbDropColumnFamilyRequest]) { request =>
                complete(dropColumnFamily(request))
              }
            }
          }
        }
      )
    }
  }

  def start(port: Int): Task[Http.ServerBinding] = {
    Task.fromFuture { _ =>
      Http().bindAndHandle(route, "0.0.0.0", port)
    }
  }

}
