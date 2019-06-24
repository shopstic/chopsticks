package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets.UTF_8

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ThrottleMode}
import akka.testkit.ImplicitSender
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.DbFactory.RemoteDbClientConfig
import dev.chopsticks.kvdb.DbTest.TestDb
import dev.chopsticks.kvdb.util.DbUtils.Implicits._
import dev.chopsticks.kvdb.util.DbUtils.{DbIndexedTailBatch, DbPair, DbTailBatch}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.proto.db._
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.{Assertion, AsyncWordSpecLike, Inside, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

object HttpDbFailureTest {
  final case class Fixture(dbClient: DbClient[TestDb.type])
  //noinspection TypeAnnotation
}

class HttpDbFailureTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inside
    //      with ParallelTestExecution
    with ImplicitSender
    with StrictLogging
    with AkkaTestKitAutoShutDown {
  import HttpDbFailureTest._

  private val as = system

  implicit object Environment extends AkkaApp.LiveEnv {
    implicit val actorSystem: ActorSystem = as
  }

  import Environment.{dispatcher, materializer}

  private def createRoute(getPath: String, flow: PartialFunction[Message, Source[Array[Byte], NotUsed]]): Route = {
    import akka.http.scaladsl.server.Directives._
    concat(
      path(getPath) {
        get {
          handleWebSocketMessages(HttpDbServer.createHandler(flow))
        }
      }
    )
  }

  private def withServerFlow(getPath: String, flow: PartialFunction[Message, Source[Array[Byte], NotUsed]])(
    testCode: Fixture => Future[Assertion]
  ): Future[Assertion] = {
    Http()
      .bindAndHandle(createRoute(getPath, flow), "0.0.0.0", 0)
      .flatMap { binding: Http.ServerBinding =>
        val serverPort = binding.localAddress.getPort
        val client: DbClient[TestDb.type] = DbFactory.client(
          TestDb,
          RemoteDbClientConfig(
            host = "localhost",
            port = serverPort,
            iterateFailureDelayResetAfter = 90.millis
          )
        )

        testCode(Fixture(client))
          .transformWith(t => binding.unbind().flatMap(_ => Future.fromTry(t)))
      }
  }

  private def testIterateSourceFlow(
    extractRequestInitialPrefix: ByteString => String,
    flow: Flow[DbPair, Array[Byte], NotUsed]
  ): PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    var failureCount = 0

    {
      case BinaryMessage.Strict(byteString) =>
        val initialPrefix = extractRequestInitialPrefix(byteString)
        logger.info(s"========= Server got request with initialPrefix = $initialPrefix")

        val source = if (initialPrefix == "" || initialPrefix == "00") {
          Source(
            Vector(
              ("0000", "0000"),
              ("0001", "0001"),
              ("0002", "0002"),
              ("0003", "0003")
            )
          ).concat(
            Source
              .lazilyAsync(
                () =>
                  akka.pattern.after(500.millis, system.scheduler)(
                    Future.failed(new RuntimeException("Some failure here") with NoStackTrace)
                  )
              )
          )
        }
        else if (initialPrefix == "0003") {
          Source(
            Vector(
              ("0004", "0004")
            )
          )
        }
        else if (initialPrefix == "0004") {
          failureCount = failureCount + 1

          if (failureCount < 5) {
            Source.failed[(String, String)](new RuntimeException("Fake failure to test backoff") with NoStackTrace)
          }
          else {
            Source(
              Vector(
                ("0005", "0005")
              )
            )
          }
        }
        else if (initialPrefix == "0005") {
          Source
            .fromFuture(akka.pattern.after(50.millis, system.scheduler) {
              Future.successful(("0006", "0006"))
            })
            .mapMaterializedValue(_ => NotUsed)
        }
        else if (initialPrefix == "0006") {
          Source(
            Vector(
              ("0007", "0007")
            )
          )
        }
        else {
          assert(initialPrefix == "0007")
          Source(
            Vector(
              ("0008", "0008")
            )
          )
        }

        source
          .log("server-out")
          .map { case (k, v) => (KvdbSerdesUtils.stringToByteArray(k), KvdbSerdesUtils.stringToByteArray(v)) }
          .via(flow)
          .log("server-block-out")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel, Logging.DebugLevel))

      case m =>
        Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
    }
  }

  "iterateSource" should {
    "retry on acceptable failure" in withServerFlow(
      HttpDbServer.ITERATE_PATH,
      testIterateSourceFlow(
        bs => DbIterateRequest.parseFrom(bs.toArray).range.from.head.operand.toString(UTF_8),
        Flow[DbPair].map(p => Array(p)).map(HttpDbServer.encodeAsDbItemBatch).map(_.toByteArray)
      )
    ) {
      case Fixture(dbClient) =>
        dbClient
          .column(_.Default)
          .source(_ ^= "00", _ ^= "00")
          .throttle(1, 1.millis, 1, ThrottleMode.shaping)
          .log("client-in")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))
          .completionTimeout(10.seconds)
          .runWith(Sink.seq)
          .map { seq =>
            seq.shouldEqual(
              Vector(
                ("0000", "0000"),
                ("0001", "0001"),
                ("0002", "0002"),
                ("0003", "0003"),
                ("0004", "0004")
              )
            )
          }
    }
  }

  "iterateValuesSource" should {
    "retry on acceptable failure" in withServerFlow(
      HttpDbServer.ITERATE_VALUES_PATH,
      testIterateSourceFlow(
        bs => DbIterateRequest.parseFrom(bs.toArray).range.from.head.operand.toString(UTF_8),
        Flow[DbPair].map(p => Array(p)).map(HttpDbServer.encodeAsDbItemValueBatch).map(_.toByteArray)
      )
    ) {
      case Fixture(dbClient) =>
        dbClient
          .column(_.Default)
          .valueSource(_ ^= "00", _ ^= "00")
          .throttle(1, 1.millis, 1, ThrottleMode.shaping)
          .log("client-in")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))
          .completionTimeout(10.seconds)
          .runWith(Sink.seq)
          .map { seq =>
            seq.shouldEqual(
              Vector(
                "0000",
                "0001",
                "0002",
                "0003",
                "0004"
              )
            )
          }
    }
  }

  "tailSource" should {
    "retry always, but complete when downstream finish" in withServerFlow(
      HttpDbServer.TAIL_PATH,
      testIterateSourceFlow(
        bs => DbIterateRequest.parseFrom(bs.toArray).range.from.head.operand.toString(UTF_8),
        Flow[DbPair]
          .map(p => Right(Array(p)).asInstanceOf[DbTailBatch])
          .map(HttpDbServer.encodeAsDbTailItemBatch)
          .map(_.toByteArray)
      )
    ) {
      case Fixture(dbClient) =>
        dbClient
          .column(_.Default)
          .tailSource(_ ^= "00", _.last)
          .throttle(1, 1.millis, 1, ThrottleMode.shaping)
          .log("client-in")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))
          .take(9)
          .completionTimeout(10.seconds)
          .runWith(Sink.seq)
          .map { seq =>
            seq.shouldEqual(
              Vector(
                ("0000", "0000"),
                ("0001", "0001"),
                ("0002", "0002"),
                ("0003", "0003"),
                ("0004", "0004"),
                ("0005", "0005"),
                ("0006", "0006"),
                ("0007", "0007"),
                ("0008", "0008")
              )
            )
          }
    }
  }

  private def testBatchTailSourceFlow(
    extractRequestInitialPrefixes: ByteString => List[String],
    flow: Flow[(Int, DbPair), Array[Byte], NotUsed]
  ): PartialFunction[Message, Source[Array[Byte], NotUsed]] = {
    var failureCount = 0

    {
      case BinaryMessage.Strict(byteString) =>
        val initialPrefixes = extractRequestInitialPrefixes(byteString)
        logger.info(s"[testBatchTailSourceFlow] Server got request with initialPrefixes = $initialPrefixes")

        val source: Source[(Int, (String, String)), NotUsed] = if (initialPrefixes == List("a", "b")) {
          Source(
            Vector(
              (0, ("a000", "a000")),
              (0, ("a001", "a001")),
              (1, ("b000", "b000")),
              (1, ("b001", "b001"))
            )
          ).concat(
            Source
              .lazilyAsync(
                () =>
                  akka.pattern.after(500.millis, system.scheduler)(
                    Future.failed(new RuntimeException("Some failure here") with NoStackTrace)
                  )
              )
          )
        }
        else if (initialPrefixes == List("a001", "b001")) {
          Source(
            Vector(
              (1, ("b002", "b002"))
            )
          )
        }
        else if (initialPrefixes == List("a001", "b002")) {
          failureCount = failureCount + 1

          if (failureCount < 5) {
            Source.failed(new RuntimeException("Fake failure to test backoff") with NoStackTrace)
          }
          else {
            Source(
              Vector(
                (1, ("b003", "b003"))
              )
            )
          }
        }
        else if (initialPrefixes == List("a001", "b003")) {
          Source
            .fromFuture(akka.pattern.after(50.millis, system.scheduler) {
              Future.successful((0, ("a002", "a002")))
            })
            .mapMaterializedValue(_ => NotUsed)
        }
        else {
          initialPrefixes should equal(List("a002", "b003"))
          Source(
            Vector(
              (0, ("a003", "a003"))
            )
          )
        }

        source
          .log("server-out")
          .map { case (i, (k, v)) => (i, (KvdbSerdesUtils.stringToByteArray(k), KvdbSerdesUtils.stringToByteArray(v))) }
          .via(flow)
          .log("server-block-out")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel, Logging.DebugLevel))

      case m =>
        Source.failed[Array[Byte]](new RuntimeException(s"Invalid message: $m"))
    }
  }

  "batchTailSource" should {
    "retry always, but complete when downstream finish" in withServerFlow(
      HttpDbServer.BATCH_TAIL_PATH,
      testBatchTailSourceFlow(
        bs => DbBatchTailRequest.parseFrom(bs.toArray).ranges.map(_.from.head.operand.toString(UTF_8)),
        Flow[(Int, DbPair)]
          .map { case (i, p) => (i, Right(Array(p))).asInstanceOf[DbIndexedTailBatch] }
          .map(HttpDbServer.encodeAsDbIndexedTailItemBatch)
          .map(_.toByteArray)
      )
    ) {
      case Fixture(dbClient) =>
        dbClient
          .column(_.Default)
          .batchTailVerboseSource(
            key =>
              List(
                (key ^= "a", key ^= "a"),
                (key ^= "b", key ^= "b")
              )
          )
          .throttle(1, 1.millis, 1, ThrottleMode.shaping)
          .log("client-in")
          .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))
          .takeWhile({
            case (0, Right(List(("a003", "a003")))) => false
            case _ => true
          }, inclusive = true)
          .completionTimeout(10.seconds)
          .runWith(Sink.seq)
          .map { seq =>
            seq.shouldEqual(
              Vector(
                (0, Right(List("a000" -> "a000"))),
                (0, Right(List("a001" -> "a001"))),
                (1, Right(List("b000" -> "b000"))),
                (1, Right(List("b001" -> "b001"))),
                (1, Right(List("b002" -> "b002"))),
                (1, Right(List("b003" -> "b003"))),
                (0, Right(List("a002" -> "a002"))),
                (0, Right(List("a003" -> "a003")))
              )
            )
          }
    }
  }

  "transaction" in withServerFlow(
    HttpDbServer.TAIL_PATH,
    testIterateSourceFlow(
      bs => DbIterateRequest.parseFrom(bs.toArray).range.from.head.operand.toString(UTF_8),
      Flow[DbPair]
        .map(p => Right(Array(p)).asInstanceOf[DbTailBatch])
        .map(HttpDbServer.encodeAsDbTailItemBatch)
        .map(_.toByteArray)
    )
  ) {
    case Fixture(dbClient) =>
      dbClient.open()
      assertDoesNotCompile("""
          |dbClient
          |  .transactionBuilder()
          |  .put(_.Default, "foo", "bar")
          |  .put(_.Lookup, "foo", "bar")
          |  .put(_.Checkpoint, "foo", "wrong type here")
          |  .encodedResult
        """.stripMargin)
  }
}
