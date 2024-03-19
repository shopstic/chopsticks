package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.grpc.scaladsl.{Metadata, PekkoGrpcClient, StreamResponseRequestBuilder}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.util.Timeout
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.types.string.NonEmptyString
import eu.timepit.refined.auto._
import io.grpc.{Status, StatusRuntimeException}
import zio.{RIO, Schedule, Scope, Tag, URIO, Unsafe, ZIO}

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.{Failure, Success}

object Dstreams {
  final case class DstreamServerConfig(
    port: Int,
    idleTimeout: Duration,
    interface: NonEmptyString = "0.0.0.0",
    shutdownTimeout: Timeout = 10.seconds
  )
  final case class DstreamClientConfig(
    serverHost: String,
    serverPort: Int,
    withTls: Boolean,
    assignmentTimeout: Timeout
  )
  final case class DstreamWorkerConfig(nodeId: String, poolSize: Int)

  val WORKER_NODE_HEADER = "dstream-worker-node"
  val WORKER_ID_HEADER = "dstream-worker-id"

  val DefaultWorkRetryPolicy: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile[Throwable] {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case _ => false
  }

  def manageServer[R](config: DstreamServerConfig)(makeHandler: URIO[R, HttpRequest => Future[HttpResponse]])
    : RIO[R with PekkoEnv with IzLogging with Scope, Http.ServerBinding] = {
    for {
      akkaSvc <- ZIO.service[PekkoEnv]
      binding <- ZIO
        .acquireRelease {
          for {
            handler <- makeHandler
            binding <- ZIO
              .fromFuture { _ =>
                import akkaSvc.actorSystem
                val settings = ServerSettings(actorSystem)

                Http()
                  .newServerAt(interface = config.interface, port = config.port)
                  .withSettings(
                    settings
                      .withTimeouts(settings.timeouts.withIdleTimeout(config.idleTimeout))
                      .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
                  )
                  .bind(handler)
              }
          } yield binding
        } { binding =>
          ZIO
            .fromFuture(_ => binding.terminate(config.shutdownTimeout.duration))
            .log("Dstream server shutdown")
            .orDie
        }
    } yield binding
  }

  def manageClientFromConfig[R, E, Client <: PekkoGrpcClient](
    config: DstreamClientConfig
  )(make: GrpcClientSettings => ZIO[R, E, Client]): ZIO[R with PekkoEnv with IzLogging with Scope, E, Client] = {
    for {
      akkaSvc <- ZIO.service[PekkoEnv]
      clientSettings <- ZIO.succeed {
        import akkaSvc.actorSystem
        GrpcClientSettings
          .connectToServiceAt(config.serverHost, config.serverPort)
          .withTls(config.withTls)
      }
      client <- manageClient(make(clientSettings))
    } yield client
  }

  def manageClient[R, E, Client <: PekkoGrpcClient](
    make: ZIO[R, E, Client]
  ): ZIO[R with IzLogging with Scope, E, Client] = {
    ZIO
      .acquireRelease(make) { client =>
        ZIO
          .fromFuture(_ => client.close())
          .orDie
      }
  }

  def distribute[Req: Tag, Res: Tag, R0, R1, A](
    makeAssignment: RIO[R0, Req]
  )(run: WorkResult[Res] => RIO[R1, A]): RIO[R0 with R1 with DstreamState[Req, Res] with PekkoEnv, A] = {
    for {
      stateSvc <- ZIO.service[DstreamState[Req, Res]]
      akkaSvc <- ZIO.service[PekkoEnv]
      assignment <- makeAssignment
      result <- {
        ZIO.acquireReleaseWith(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
          stateSvc
            .report(assignmentId)
            .flatMap {
              case None => ZIO.unit
              case Some(worker) =>
                ZIO
                  .succeed {
                    import akkaSvc.actorSystem
                    worker.source.runWith(Sink.cancelled)
                  }
                  .ignore
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

  def work[R, Req, Res](
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req],
    initialTimeout: FiniteDuration = 5.seconds,
    retryPolicy: Schedule[Any, Throwable, Any] = DefaultWorkRetryPolicy
  )(makeSource: Req => RIO[R, Source[Res, NotUsed]]): RIO[PekkoEnv with IzLogging with R, Unit] = {
    val task = for {
      promise <- ZIO.succeed(Promise[Source[Res, NotUsed]]())
      result <- requestBuilder
        .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
        .toZAkkaSource
        .killSwitch
        .viaBuilder(_.initialTimeout(initialTimeout))
        .mapAsync(1) {
          assignment =>
            makeSource(assignment)
              .map(s => promise.success(s))
              .zipRight(ZIO.fromFuture(_ => promise.future))
        }
        .interruptibleRunIgnore()
    } yield result

    task.retry(retryPolicy)
  }

  @nowarn("cat=lint-infer-any")
  def workPool[Req, Res, R <: PekkoEnv](
    config: DstreamWorkerConfig,
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(
    makeSource: Req => RIO[R, Source[Res, NotUsed]]
  ): RIO[R with IzLogging, Unit] = {
    ZIO
      .foreachPar((1 to config.poolSize).toList) { id =>
        work(requestBuilder.addHeader(WORKER_ID_HEADER, id.toString).addHeader(WORKER_NODE_HEADER, config.nodeId))(
          makeSource
        ).logResult(s"dstream-worker-$id", _.toString)
          .forever
          .retry(Schedule.exponential(100.millis) || Schedule.fixed(1.second))
      }(List, implicitly[zio.Trace])
      .unit
  }

  def handle[Req, Res](
    stateService: DstreamState[Req, Res],
    in: Source[Res, NotUsed],
    metadata: Metadata
  )(implicit rt: zio.Runtime[PekkoEnv]): Source[Req, NotUsed] = {
    val pekkoSvc = rt.environment.get
    import pekkoSvc.{actorSystem, dispatcher}

    val (ks, inSource) = in
      .viaMat(KillSwitches.single)(Keep.right)
      .preMaterialize()

    val futureSource = Unsafe.unsafe { implicit unsafe =>
      rt.unsafe.runToFuture(stateService.enqueueWorker(inSource, metadata))
    }

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
