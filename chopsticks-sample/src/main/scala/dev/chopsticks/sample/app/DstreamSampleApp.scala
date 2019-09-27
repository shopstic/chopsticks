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
    val workerGauge: Gauge = Gauge.build.name("dstream_workers").register()
    val attemptCounter: Counter = Counter.build.name("dstream_attempts_total").register()
    val queueGauge: Gauge = Gauge.build.name("dstream_queue").register()
    val mapGauge: Gauge = Gauge.build.name("dstream_map").register()
  }

  protected def createEnv(untypedConfig: Config) = {
    ZManaged.environment[AkkaApp.Env].map { env =>
      val akkaEnv = env.akka
      import akkaEnv._
      new AkkaApp.LiveEnv with DsEnv {
        val akka: AkkaEnv.Service = env.akka
        object dstreamService extends DstreamEnv.LiveService[Assignment, Result](rt, dstreamMetrics) {
          env.akka
            .unsafeRunToFuture(updateQueueGauge.repeat(ZSchedule.fixed(1.second)).provide(env))
            .onComplete(t => s"METRICS COMLETED ===================== $t")
        }
      }
    }
  }

  private val runServer = {
    val graphTask = ZIO.access[AkkaEnv with DsEnv with MeasuredLogging] { implicit env =>
      val ks = KillSwitches.shared("server shared killswitch")

      Source(1 to Int.MaxValue)
        .map(Assignment(_))
        .via(ZAkkaStreams.interruptableMapAsyncUnordered(12) { assignment: Assignment =>
          Dstreams
            .distribute(assignment) { result: WorkResult[Result] =>
              Task.fromFuture { _ =>
                result.source
                  .via(ks.flow)
                  .take(1)
                  .runForeach { _ =>
                    //                    println(s"Server < [worker=$workerId][assignment=${assignment.valueIn}] $r")
                  }(env.akka.materializer)
              }
            }
            .retry(ZSchedule.logInput((e: Throwable) => ZLogger.error("Distribute failed", e)))
        })
        .via(ks.flow)
        //        .wireTap(a => println(s"Server < completed $a"))
        .toMat(Sink.ignore)(Keep.right)
        .mapMaterializedValue(f => (ks, f))
    }

    ZAkkaStreams.interruptableGraphM(graphTask, graceful = true)
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

  protected def run: ZIO[Env, Throwable, Unit] = {
    val createService = ZIO.access[AkkaEnv with DsEnv] { env =>
      val akkaEnv = env.akka
      import akkaEnv._
      DstreamSampleAppPowerApiHandler(Dstreams.handle(env, _, _))
    }
    val port = 9999
    val managedServer =
      Dstreams.createManagedServer(DstreamServerConfig(port = port, idleTimeout = 5.seconds), createService)
    val managedClient = Dstreams.createManagedClient(ZIO.access[AkkaEnv](_.akka).map { env =>
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
