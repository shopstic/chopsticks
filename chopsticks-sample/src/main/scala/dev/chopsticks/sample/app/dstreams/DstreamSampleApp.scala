package dev.chopsticks.sample.app.dstreams

import akka.Done
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetricsGroup
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.TaskUtils
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, _}
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstreams.proto.dstream_sample_app._
import dev.chopsticks.stream.ZAkkaStreams
import io.grpc.StatusRuntimeException
import io.prometheus.client.CollectorRegistry
import zio._

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamSampleApp extends AkkaDiApp[Unit] {

  private type Env = AkkaEnv with DstreamStateFactory with MeasuredLogging

  private lazy val serviceId = "dstream_sample_app"

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Unit,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetricsGroup](serviceId),
        DstreamStateMetricsManager.live,
        DstreamStateFactory.live,
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  def app = {
    val managedZio = for {
      dstreamStateFactory <- ZManaged.access[DstreamStateFactory](_.get)
      dstreamState <- dstreamStateFactory.createStateService[Assignment, Result](serviceId)
    } yield {
      for {
        runEnv <- ZIO.environment[Env]
        logEnv <- ZIO.environment[MeasuredLogging with DstreamStateMetricsManager]
        _ <- TaskUtils.raceFirst(
          List(
            "Run task" -> run(dstreamState).provide(runEnv),
            "Periodic logging" -> periodicallyLog.provide(logEnv)
          )
        )
      } yield ()
    }
    managedZio.use(identity)
  }

  private def periodicallyLog = {
    val io = for {
      logger <- ZIO.access[IzLogging](_.get).map(_.zioLogger)
      dstreamMetrics <- ZIO.accessM[DstreamStateMetricsManager](_.get.activeSet)
      metrics <- UIO {
        ListMap(
          "workerGauge" -> dstreamMetrics.iterator.map(_.dstreamWorkerGauge.get).sum.toString,
          "attemptCounter" -> dstreamMetrics.iterator.map(_.dstreamAttemptCounter.get).sum.toString,
          "queueGauge" -> dstreamMetrics.iterator.map(_.dstreamQueueGauge.get).sum.toString,
          "mapGauge" -> dstreamMetrics.iterator.map(_.dstreamMapGauge.get).sum.toString
        )
      }
      formatted = metrics.iterator.map { case (k, v) => s"$k=$v" }.mkString(" ")
      _ <- logger.info(s"${formatted -> "formatted" -> null}")
    } yield ()
    io.repeat(Schedule.fixed(1.second.toJava)).unit
  }

  private def runServer(dstreamState: DstreamState.Service[Assignment, Result])
    : RIO[AkkaEnv with MeasuredLogging, Done] = {
    for {
      graph <- ZIO.runtime[AkkaEnv with MeasuredLogging].map { implicit rt =>
        val ks = KillSwitches.shared("server shared killswitch")
        implicit val as: ActorSystem = rt.environment.get[AkkaEnv.Service].actorSystem

        Source(1 to 100)
          .map(Assignment(_))
          .via(ZAkkaStreams.interruptibleMapAsyncUnordered(12) { assignment: Assignment =>
            Dstreams
              .distribute(dstreamState)(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
                Task.fromFuture { _ =>
                  result
                    .source
                    .via(ks.flow)
                    .take(1)
                    .runForeach { _ =>
                      println(
//                        s"Server < [worker=${result.metadata.getText(Dstreams.WORKER_ID_HEADER)}][assignment=${assignment.valueIn}] $r"
                      )
                    }
                }
              }
              .retry(Schedule.forever.tapInput((e: Throwable) =>
                ZIO.accessM[IzLogging](_.get.zioLogger.error(s"Distribute failed: $e"))
              ))
          })
          .via(ks.flow)
          //        .wireTap(a => println(s"Server < completed $a"))
          .toMat(Sink.ignore)(Keep.right)
          .mapMaterializedValue(f => (ks, f))
      }

      result <- ZAkkaStreams.interruptibleGraph(graph, graceful = true)
    } yield result
  }

  protected def runWorker(client: DstreamSampleAppClient, id: Int) = {
    Dstreams
      .work(client.work().addHeader(Dstreams.WORKER_ID_HEADER, id.toString)) { a =>
        ZIO.access[AkkaEnv] { _ =>
          Source
            .single(1)
            .map(v => Result(a.valueIn * 10 + v))
        //            .delay(1.second, DelayOverflowStrategy.backpressure)
        //            .throttle(1, 1.second)
        //            .wireTap(r => println(s"Client > [worker=$id][assignment=${a.valueIn}] $r"))
        //            .map { v =>
        //              if (math.random() > 0.9) {
        //                throw new IllegalStateException("test worker death")
        //              }
        //              vE
        //            }
        }
      }
      .log(s"Running worker $id")
  }

  def run(dstreamState: DstreamState.Service[Assignment, Result]): RIO[Env, Unit] = {
    val createService = ZIO.runtime[AkkaEnv].map { implicit rt =>
      val akkaEnv = rt.environment.get[AkkaEnv.Service]
      import akkaEnv.actorSystem
      DstreamSampleAppPowerApiHandler {
        (source, metadata) =>
          Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
      }
    }
    val port = 9999
    val managedServer =
      Dstreams.createManagedServer(DstreamServerConfig(port = port, idleTimeout = 30.seconds), createService)
    val managedClient = Dstreams.createManagedClient(ZIO.access[AkkaEnv](_.get).map { env =>
      import env.actorSystem
      DstreamSampleAppClient(
        GrpcClientSettings
          .connectToServiceAt("localhost", port)
          .withTls(false)
//          .withChannelBuilderOverrides(
//            _.eventLoopGroup(new io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup(4))
//              .channelType(classOf[io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel])
//              .executor(env.dispatcher)
//          )
      )
    })
    val resources = managedServer zip managedClient

    resources.use {
      case (_, client) =>
        for {
          s <- runServer(dstreamState)
            .log("server graph")
            .fork
          _ <- ZIO.forkAll_ {
            (1 to 8).map { id =>
              runWorker(client, id)
                .foldM(
                  {
                    case e: StatusRuntimeException => ZIO.fail(e)
                    case e => ZIO.left(e)
                  },
                  r => ZIO.right(r)
                )
                .repeat(Schedule.fixed(1.second.toJava))
            }
          }
          _ <- s.join
        } yield ()
    }
  }
}
