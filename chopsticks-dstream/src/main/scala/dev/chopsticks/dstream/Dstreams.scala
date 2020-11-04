package dev.chopsticks.dstream

import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.{AkkaGrpcClient, Metadata, StreamResponseRequestBuilder}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import dev.chopsticks.dstream.DstreamEnv.WorkResult
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp._
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.stream.ZAkkaStreams
import zio._

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
      binding <- ZIO.access[AkkaEnv](_.get[AkkaEnv.Service]).flatMap { env =>
        import env.actorSystem
        val settings = ServerSettings(actorSystem)

        Task
          .fromFuture { _ =>
            Http().bindAndHandleAsync(
              service,
              interface = "0.0.0.0",
              port = config.port,
              settings = settings
                .withTimeouts(settings.timeouts.withIdleTimeout(config.idleTimeout))
                .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
            )
          }
      }
    } yield binding

    ZManaged
      .make(
        acquire
          .logResult("dstream server startup", b => s"Dstream server bound: ${b.localAddress}")
      ) { binding =>
        ZIO.access[IzLogging](_.get).map(_.zioLogger).flatMap { zLogger =>
          Task
            .fromFuture(_ => binding.terminate(10.seconds))
            .log("dstream server teardown")
            .unit
            .catchAll(e => zLogger.error(s"Failed unbinding dstream server: $e"))
        }
      }
  }

  def createManagedClientFromConfig[R, E, Client <: AkkaGrpcClient](
    config: DstreamClientConfig
  )(make: GrpcClientSettings => ZIO[R, E, Client]): ZManaged[R with AkkaEnv with MeasuredLogging, E, Client] = {
    ZManaged.make(
      ZIO
        .access[AkkaEnv](_.get[AkkaEnv.Service])
        .map { env =>
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
      ZIO.access[IzLogging](_.get).map(_.zioLogger).flatMap { zLogger =>
        Task
          .fromFuture(_ => client.close())
          .log("dstream client teardown")
          .unit
          .catchAll(e => zLogger.error(s"Failed closing dstream client: $e"))
      }
    }
  }

  def createManagedClient[R, E, Client <: AkkaGrpcClient](
    make: => ZIO[R, E, Client]
  ): ZManaged[R with MeasuredLogging, E, Client] = {
    ZManaged
      .make(
        make
          .logResult("dstream client Startup", _ => s"dstream client created")
      ) { client =>
        ZIO.access[IzLogging](_.get).map(_.zioLogger).flatMap { zLogger =>
          Task
            .fromFuture(_ => client.close())
            .log("dstream client teardown")
            .unit
            .catchAll(e => zLogger.error(s"Failed closing dstream client: $e"))
        }
      }
  }

  def distribute[Req: Tag, Res: Tag, R <: AkkaEnv, A](
    assignment: Req
  )(run: WorkResult[Res] => RIO[R, A]): RIO[R with DstreamEnv[Req, Res], A] = {
    ZIO
      .bracket {
        ZIO
          .accessM[DstreamEnv[Req, Res]](_.get[DstreamEnv.Service[Req, Res]].enqueueAssignment(assignment))
      } { worker =>
        val cleanup = for {
          _ <- ZIO.access[AkkaEnv](_.get[AkkaEnv.Service].actorSystem).map { implicit as =>
            worker.source.runWith(Sink.cancelled)
          }
          _ <- ZIO.accessM[DstreamEnv[Req, Res]](_.get.report(assignment))
        } yield ()

        cleanup.orDie
      }(run)
  }

  def work[R <: Has[_], Req, Res](
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(makeSource: Req => RIO[R, Source[Res, NotUsed]]): RIO[AkkaEnv with LogEnv with R, Done] = {
    for {
      graph <- ZIO.runtime[AkkaEnv with R].map { implicit rt =>
        val promise = Promise[Source[Res, NotUsed]]()
        requestBuilder
          .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
          .viaMat(KillSwitches.single)(Keep.right)
          .via(ZAkkaStreams.interruptibleMapAsync(1) { assignment: Req =>
            makeSource(assignment)
              .map(s => promise.success(s))
              .zipRight(Task.fromFuture(_ => promise.future))
          })
          .toMat(Sink.ignore)(Keep.both)
      }
      result <- ZAkkaStreams.interruptibleGraph(graph, graceful = true)
    } yield result
  }

  def workPool[Req, Res, R <: AkkaEnv](
    config: DstreamWorkerConfig,
    requestBuilder: => StreamResponseRequestBuilder[Source[Res, NotUsed], Req]
  )(
    makeSource: Req => RIO[R, Source[Res, NotUsed]]
  ): ZIO[R with MeasuredLogging with LogEnv, Throwable, Unit] = {
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
    in: Source[Res, NotUsed],
    metadata: Metadata
  )(implicit rt: zio.Runtime[AkkaEnv with DstreamEnv[Req, Res]]): Source[Req, NotUsed] = {
    val env = rt.environment
    val akkaService = env.get[AkkaEnv.Service]
    import akkaService.{actorSystem, dispatcher}

    val (ks, inSource) = in
      .viaMat(KillSwitches.single)(Keep.right)
      .preMaterialize()

    val futureSource =
      env
        .get[DstreamEnv.Service[Req, Res]]
        .enqueueWorker(inSource, metadata)
        .unsafeRunToFuture(rt)

    Source
      .futureSource(futureSource)
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
