package dev.chopsticks.sample.app

import akka.grpc.GrpcClientSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamEnv.WorkResult
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream.{DstreamEnv, Dstreams}
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, _}
import dev.chopsticks.fp.{AkkaApp, AkkaEnv, ZLogger}
import dev.chopsticks.sample.app.proto.dstream_sample_app._
import dev.chopsticks.stream.ZAkkaStreams
import io.prometheus.client.{Counter, Gauge}
import zio._

import scala.concurrent.duration._

object DstreamSampleApp extends AkkaApp {
  type DsEnv = DstreamEnv[Assignment, Result]
  type Env = AkkaApp.Env with DsEnv

  private object dstreamMetrics extends DstreamEnv.Metrics {
    val workerGauge: Gauge = Gauge.build("dstream_workers", "dstream_workers").register()
    val attemptCounter: Counter = Counter.build("dstream_attempts_total", "dstream_attempts_total").register()
    val queueGauge: Gauge = Gauge.build("dstream_queue", "dstream_queue").register()
    val mapGauge: Gauge = Gauge.build("dstream_map", "dstream_map").register()
  }

  protected def createEnv(untypedConfig: Config) = {
    for {
      env <- ZManaged.environment[AkkaApp.Env]
      rt <- ZManaged.fromEffect(ZIO.runtime[Any])
    } yield {
      val akkaEnv = env.akkaService
      import akkaEnv._
      new AkkaApp.LiveEnv with DsEnv {
        val akkaService: AkkaEnv.Service = env.akkaService
        object dstreamService extends DstreamEnv.LiveService[Assignment, Result](rt, dstreamMetrics) {
          rt.unsafeRunToFuture(updateQueueGauge.repeat(ZSchedule.fixed(1.second)).provide(env))
            .onComplete(t => s"METRICS COMLETED ===================== $t")
        }
      }
    }
  }

  private val runServer = {
    val graphTask = ZIO.runtime[AkkaEnv with DsEnv with MeasuredLogging].map { implicit rt =>
      val ks = KillSwitches.shared("server shared killswitch")

      Source(1 to Int.MaxValue)
        .map(Assignment(_))
        .via(ZAkkaStreams.interruptibleMapAsyncUnordered(12) { assignment: Assignment =>
          Dstreams
            .distribute(assignment) { result: WorkResult[Result] =>
              Task.fromFuture { _ =>
                result.source
                  .via(ks.flow)
                  .take(1)
                  .runForeach { _ =>
                    //                    println(s"Server < [worker=$workerId][assignment=${assignment.valueIn}] $r")
                  }(rt.Environment.akkaService.materializer)
              }
            }
            .retry(ZSchedule.logInput((e: Throwable) => ZLogger.error("Distribute failed", e)))
        })
        .via(ks.flow)
        //        .wireTap(a => println(s"Server < completed $a"))
        .toMat(Sink.ignore)(Keep.right)
        .mapMaterializedValue(f => (ks, f))
    }

    ZAkkaStreams.interruptibleGraph(graphTask, graceful = true)
  }

  protected def runWorker(client: DstreamSampleAppClient, id: Int) = {
    Dstreams
      .work(client.work().addHeader(Dstreams.WORKER_ID_HEADER, id.toString)) { a =>
        ZIO.access[AkkaEnv] { _ =>
          //          println(s"Client < [worker=$id][assignment=${a.valueIn}]")
          Source
            .single(1)
            .map(v => Result(a.valueIn * 10 + v))
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
  }

  def run: ZIO[Env, Throwable, Unit] = {
    val createService = ZIO.runtime[AkkaEnv with DsEnv].map { rt =>
      val akkaEnv = rt.Environment.akkaService
      import akkaEnv._
      DstreamSampleAppPowerApiHandler(Dstreams.handle(rt, _, _))
    }
    val port = 9999
    val managedServer =
      Dstreams.createManagedServer(DstreamServerConfig(port = port, idleTimeout = 5.seconds), createService)
    val managedClient = Dstreams.createManagedClient(ZIO.access[AkkaEnv](_.akkaService).map { env =>
      import env._
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
        val task = for {
          s <- runServer
            .log("server graph")
            .fork
          _ <- ZIO.forkAll_ {
            (1 to 8).map { id =>
              runWorker(client, id).either
                .repeat(ZSchedule.forever)
            }
          }
          _ <- s.join
        } yield ()

        task.interruptChildren
    }
  }
}
