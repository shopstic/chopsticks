package dev.chopsticks.dstream

import akka.{Done, NotUsed}
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.{AkkaGrpcClient, Metadata, StreamResponseRequestBuilder}
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.dstream.DstreamEnv.WorkResult
import dev.chopsticks.fp.ZIOExt.MeasuredLogging
import dev.chopsticks.fp.ZIOExt.Implicits._
import dev.chopsticks.fp.{AkkaEnv, LoggingContext, ZIOExt, ZLogger}
import zio.clock.Clock
import zio.{Task, TaskR, ZIO, ZManaged, ZSchedule}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object Dstreams extends LoggingContext {
  final case class DstreamServerConfig(port: Int, idleTimeout: Duration)
  final case class DstreamClientConfig(serverHost: String, serverPort: Int, withTls: Boolean)
  final case class DstreamWorkerConfig(nodeId: String, poolSize: Int)

  val WORKER_NODE_HEADER = "dstream-worker-node"
  val WORKER_ID_HEADER = "dstream-worker-id"

  def createManagedServer[R](
    config: DstreamServerConfig,
    makeService: ZIO[R, Nothing, HttpRequest => Future[HttpResponse]]
  ): ZManaged[R with AkkaEnv with MeasuredLogging, Throwable, Http.ServerBinding] = {
    val acquire = for {
      service <- makeService
      binding <- ZIO.accessM[AkkaEnv] { env =>
        import env._
        val settings = ServerSettings(actorSystem)

        Task
          .fromFuture { _ =>
            Http().bindAndHandleAsync(
              service,
              interface = "0.0.0.0",
              port = config.port,
              connectionContext = HttpConnectionContext(http2 = Always),
              settings = settings
                .withTimeouts(settings.timeouts.withIdleTimeout(config.idleTimeout))
                .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
            )
          }
      }
    } yield binding

    ZManaged.make(
      acquire
        .logResult("dstream server startup", b => s"Dstream server bound: ${b.localAddress}")
    ) { binding =>
      Task
        .fromFuture(_ => binding.terminate(10.seconds))
        .log("dstream server teardown")
        .map(_ => ())
        .catchAll(e => ZLogger.error("Failed unbinding dstream server", e))
    }
  }

  def createManagedClientFromConfig[R, E, Client <: AkkaGrpcClient](
    config: DstreamClientConfig
  )(make: GrpcClientSettings => ZIO[R, E, Client]): ZManaged[R with AkkaEnv with MeasuredLogging, E, Client] = {
    ZManaged.make(
      ZIO
        .access[AkkaEnv] { env =>
          import env._
          GrpcClientSettings
            .connectToServiceAt(config.serverHost, config.serverPort)
            .withTls(config.withTls)
//            .withChannelBuilderOverrides(
//              _.eventLoopGroup(new io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup(1))
//                .executor(env.dispatcher)
//            )
        }
        .flatMap(make)
        .logResult("dstream client Startup", _ => s"dstream client created")
    ) { client =>
      Task
        .fromFuture(_ => client.close())
        .log("dstream client teardown")
        .map(_ => ())
        .catchAll(e => ZLogger.error("Failed closing dstream client", e))
    }
  }

  def createManagedClient[R, E, Client <: AkkaGrpcClient](
    make: => ZIO[R, E, Client]
  ): ZManaged[R with MeasuredLogging, E, Client] = {
    ZManaged.make(
      make
        .logResult("dstream client Startup", _ => s"dstream client created")
    ) { client =>
      Task
        .fromFuture(_ => client.close())
        .log("dstream client teardown")
        .map(_ => ())
        .catchAll(e => ZLogger.error("Failed closing dstream client", e))
    }
  }

  def distribute[Req, Res, R <: AkkaEnv, A](
    assignment: Req
  )(run: WorkResult[Res] => TaskR[R, A]): TaskR[R with DstreamEnv[Req, Res], A] = {
    ZIO
      .bracket {
        ZIO
          .accessM[DstreamEnv[Req, Res]](_.dstreamService.enqueueAssignment(assignment))
      } { worker =>
        val cleanup = for {
          _ <- ZIO.access[AkkaEnv](env => worker.source.runWith(Sink.cancelled)(env.materializer))
          _ <- ZIO.accessM[DstreamEnv[Req, Res]](_.dstreamService.report(assignment))
        } yield ()

        cleanup.catchAll(ZIO.die)
      }(run)
  }

  def work[Req, Res, R <: AkkaEnv](
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(makeSource: Req => TaskR[R, Source[Res, NotUsed]]): TaskR[R, Done] = {
    val graph = ZIO.access[R] { implicit env =>
      val promise = Promise[Source[Res, NotUsed]]()

      requestBuilder
        .invoke(Source.fromFutureSource(promise.future).mapMaterializedValue(_ => NotUsed))
        .viaMat(KillSwitches.single)(Keep.right)
        .via(ZIOExt.interruptableMapAsync(1) { assignment: Req =>
          makeSource(assignment).map(s => promise.success(s)) *> Task.fromFuture(_ => promise.future)
        })
        .toMat(Sink.ignore)(Keep.both)
    }

    ZIOExt.interruptableGraph(graph, graceful = false)
  }

  def workPool[Req, Res, R <: AkkaEnv](
    config: DstreamWorkerConfig,
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(
    makeSource: Req => TaskR[R, Source[Res, NotUsed]]
  ): ZIO[R with MeasuredLogging with Clock, Throwable, Unit] = {
    val retrySchedule: ZSchedule[Clock, Throwable, Any] = ZSchedule.exponential(100.millis) || ZSchedule.fixed(1.second)

    ZIO
      .foreachPar(1 to config.poolSize) { id =>
        work(requestBuilder.addHeader(WORKER_ID_HEADER, id.toString).addHeader(WORKER_NODE_HEADER, config.nodeId))(
          makeSource
        ).logResult(s"dstream-worker-$id", _.toString)
          .forever
          .retry(retrySchedule)
      }
      .const(())
  }

  def handle[Req, Res](
    env: AkkaEnv with DstreamEnv[Req, Res],
    in: Source[Res, NotUsed],
    metadata: Metadata
  ): Source[Req, NotUsed] = {
    import env._

    val (ks, inSource) = in
      .viaMat(KillSwitches.single)(Keep.right)
      .preMaterialize()

    Source
      .fromFutureSource(unsafeRunToFuture(dstreamService.enqueueWorker(inSource, metadata)))
      .watchTermination() {
        case (_, f) =>
          f.onComplete {
            case Success(_) => ks.shutdown()
            case Failure(ex) => ks.abort(ex)
          }
          NotUsed
      }
  }
}
