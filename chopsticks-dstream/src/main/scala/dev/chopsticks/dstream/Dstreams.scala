package dev.chopsticks.dstream

import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.{AkkaGrpcClient, Metadata, StreamResponseRequestBuilder}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.stream.{ZAkkaFlow, ZAkkaStreams}
import io.grpc.{Status, StatusRuntimeException}
import zio._
import zio.clock.Clock

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.{Failure, Success}

object Dstreams {
  final case class DstreamServerConfig(port: Int, idleTimeout: Duration)
  final case class DstreamClientConfig(serverHost: String, serverPort: Int, withTls: Boolean)
  final case class DstreamWorkerConfig(nodeId: String, poolSize: Int)

  val WORKER_NODE_HEADER = "dstream-worker-node"
  val WORKER_ID_HEADER = "dstream-worker-id"

  val DefaultWorkRetryPolicy: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile[Throwable] {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case _ => false
  }

  def manageServer[R](config: DstreamServerConfig)(makeHandler: URIO[R, HttpRequest => Future[HttpResponse]])
    : RManaged[R with AkkaEnv with MeasuredLogging, Http.ServerBinding] = {
    for {
      akkaSvc <- ZManaged.access[AkkaEnv](_.get)
      binding <- ZManaged
        .make {
          for {
            handler <- makeHandler
            binding <- Task
              .fromFuture { _ =>
                import akkaSvc.actorSystem
                val settings = ServerSettings(actorSystem)

                Http()
                  .newServerAt(interface = "0.0.0.0", port = config.port)
                  .withSettings(
                    settings
                      .withTimeouts(settings.timeouts.withIdleTimeout(config.idleTimeout))
                      .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
                  )
                  .bind(handler)
              }
          } yield binding
        } { binding =>
          Task
            .fromFuture(_ => binding.terminate(10.seconds))
            .log("Dstream server teardown")
            .orDie
        }
    } yield binding
  }

  def manageClientFromConfig[R, E, Client <: AkkaGrpcClient](
    config: DstreamClientConfig
  )(make: GrpcClientSettings => ZIO[R, E, Client]): ZManaged[R with AkkaEnv with MeasuredLogging, E, Client] = {
    for {
      akkaSvc <- ZManaged.access[AkkaEnv](_.get)
      clientSettings <- ZManaged.effectTotal {
        import akkaSvc.actorSystem
        GrpcClientSettings
          .connectToServiceAt(config.serverHost, config.serverPort)
          .withTls(config.withTls)
      }
      client <- manageClient(make(clientSettings))
    } yield client
  }

  def manageClient[R, E, Client <: AkkaGrpcClient](
    make: ZIO[R, E, Client]
  ): ZManaged[R with MeasuredLogging, E, Client] = {
    ZManaged
      .make(make) { client =>
        Task
          .fromFuture(_ => client.close())
          .orDie
      }
  }

  def distribute[Req: Tag, Res: Tag, R0, R1, A](
    makeAssignment: RIO[R0, Req]
  )(run: WorkResult[Res] => RIO[R1, A]): RIO[R0 with R1 with DstreamState[Req, Res] with AkkaEnv, A] = {
    for {
      stateSvc <- ZIO.access[DstreamState[Req, Res]](_.get)
      akkaSvc <- ZIO.access[AkkaEnv](_.get)
      assignment <- makeAssignment
      result <- {
        ZIO.bracket(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
          stateSvc
            .report(assignmentId)
            .flatMap {
              case None => ZIO.unit
              case Some(worker) =>
                UIO {
                  import akkaSvc.actorSystem
                  worker.source.runWith(Sink.cancelled)
                }.ignore
            }
            .orDie
        } { assignmentId =>
          stateSvc
            .awaitForWorker(assignmentId)
            .flatMap(run)
        }
      }
    } yield result
  }

  def work[R <: Has[_], Req, Res](
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req],
    initialTimeout: FiniteDuration = 5.seconds,
    retryPolicy: Schedule[Any, Throwable, Any] = DefaultWorkRetryPolicy
  )(makeSource: Req => RIO[R, Source[Res, NotUsed]]): RIO[AkkaEnv with IzLogging with Clock with R, Done] = {
    val task = for {
      promise <- UIO(Promise[Source[Res, NotUsed]]())
      flow <- ZAkkaFlow[Req].interruptibleMapAsync(1) { assignment =>
        makeSource(assignment)
          .map(s => promise.success(s))
          .zipRight(Task.fromFuture(_ => promise.future))
      }
      result <- ZAkkaStreams.interruptibleGraph(
        requestBuilder
          .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
          .viaMat(KillSwitches.single)(Keep.right)
          .initialTimeout(initialTimeout)
          .via(flow)
          .toMat(Sink.ignore)(Keep.both),
        graceful = true
      )
    } yield result

    task.retry(retryPolicy)
  }

  def workPool[Req, Res, R <: AkkaEnv](
    config: DstreamWorkerConfig,
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(
    makeSource: Req => RIO[R, Source[Res, NotUsed]]
  ): RIO[R with MeasuredLogging, Unit] = {
    ZIO
      .foreachPar((1 to config.poolSize).toList) { id =>
        work(requestBuilder.addHeader(WORKER_ID_HEADER, id.toString).addHeader(WORKER_NODE_HEADER, config.nodeId))(
          makeSource
        ).logResult(s"dstream-worker-$id", _.toString)
          .forever
          .retry(Schedule.exponential(100.millis) || Schedule.fixed(1.second))
      }(List)
      .unit
  }

  def handle[Req: Tag, Res: Tag](
    stateService: DstreamState.Service[Req, Res],
    in: Source[Res, NotUsed],
    metadata: Metadata
  )(implicit rt: zio.Runtime[AkkaEnv]): Source[Req, NotUsed] = {
    val akkaSvc = rt.environment.get
    import akkaSvc.{actorSystem, dispatcher}

    val (ks, inSource) = in
      .viaMat(KillSwitches.single)(Keep.right)
      .preMaterialize()

    val futureSource = rt.unsafeRunToFuture(stateService.enqueueWorker(inSource, metadata))

    Source
      .futureSource(futureSource)
      .watchTermination() {
        case (_, f) =>
          f
            .transformWith { result =>
              futureSource
                .cancel()
                .map(exit => result.flatMap(_ => exit.toEither.toTry))
            }
            .onComplete {
              case Success(_) => ks.shutdown()
              case Failure(ex) => ks.abort(ex)
            }
          NotUsed
      }
  }
}
