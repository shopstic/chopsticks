package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.SharedResourceManager
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.MetricConfigs.{LabelValues, _}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.IbGatewayProxyDebug.{
  IbGatewayProxyConfig,
  IbGatewayProxyMetric,
  MM,
  Metrics,
  MetricsFactory,
  MetricsManager
}
//import eu.timepit.refined.auto._
//import eu.timepit.refined.types.net.PortNumber
//import eu.timepit.refined.types.string.NonEmptyString
import zio._
import zio.clock.Clock

import scala.concurrent.duration._

object IbGatewayProxyDebug {
  object Labels {
    final case object target extends MetricLabel
    final case object fromPort extends MetricLabel
    final case object toPort extends MetricLabel
  }

  sealed trait IbGatewayProxyMetric extends MetricGroup
  final case object clientCount
      extends GaugeConfig(LabelNames of Labels.target and Labels.fromPort and Labels.toPort)
      with IbGatewayProxyMetric

  final case class IbGatewayProxyConfig(foo: String)

  final case class Metrics(config: IbGatewayProxyConfig, activeClientGauge: MetricGauge)

  final class MetricsFactory extends MetricServiceFactory[IbGatewayProxyMetric, IbGatewayProxyConfig, Metrics] {
    override def create(
      registry: MetricRegistry.Service[IbGatewayProxyMetric],
      config: IbGatewayProxyConfig
    ): Metrics = {
      val activeClientGauge = registry.gaugeWithLabels(
        clientCount,
        LabelValues
          .of(Labels.target -> "")
          .and(Labels.fromPort -> "")
          .and(Labels.toPort -> "")
      )

      Metrics(IbGatewayProxyConfig("foo"), activeClientGauge)
    }
  }

  type MM = SharedResourceManager.Service[Any, IbGatewayProxyConfig, Metrics]
  type MetricsManager = Has[MM]

//  implicit lazy val cfgTag = implicitly[zio.Tag[IbGatewayProxyConfig]]
//  implicit lazy val managerTag = implicitly[zio.Tag[MM]]

  trait Service {
    def run(runConfig: IbGatewayProxyConfig): UManaged[Unit]
  }

  lazy val tt1 = implicitly[zio.Tag[MM]]

  def live: ZLayer[AkkaEnv with IzLogging with MetricsManager, Nothing, Has[Service]] = {
    val managed = for {
      metricsManager <- ZManaged.access[MetricsManager](_.get)
    } yield {
      new Service {
        override def run(runConfig: IbGatewayProxyConfig): UManaged[Unit] = {
          metricsManager.manage(IbGatewayProxyConfig("bar")).ignore
        }
      }
    }

    managed.toLayer
  }
}

object MetricTestApp extends AkkaDiApp[Unit] {

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(akkaAppDi: DiModule, appConfig: Unit, allConfig: Config): Task[DiEnv[AppEnv]] = {
    val tt2 = implicitly[zio.Tag[MM]]

    val _ = tt2.tag.equals(IbGatewayProxyDebug.tt1.tag)
    assert(tt2.tag == IbGatewayProxyDebug.tt1.tag)

    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.succeed(PromMetricRegistry.live[IbGatewayProxyMetric]("test")),
          MetricServiceManager.live[IbGatewayProxyMetric, IbGatewayProxyConfig, Metrics](new MetricsFactory),
          IbGatewayProxyDebug.live,
          AppLayer(app)
        )
      )
    }
  }

  def app: URIO[Clock with Has[IbGatewayProxyDebug.Service] with MetricsManager, Unit] = {
    val managed = for {
      metricsManager <- ZManaged.service[MM]
      _ <- ZManaged.service[IbGatewayProxyDebug.Service]
    } yield {
      for {
        _ <-
          metricsManager
            .manage(IbGatewayProxyConfig("bar"))
            .use { metrics =>
              UIO {
                metrics.activeClientGauge.inc()
              } *> ZIO.unit.delay(1.second)
            }
            .zipRight(ZIO.unit.delay(1.second))
            .repeat(Schedule.forever)
            .fork
        _ <- metricsManager
          .activeSet
          .map { activeSet =>
            if (activeSet.nonEmpty) {
              activeSet.foreach { m =>
                println(s"activeClientGauge = ${m.activeClientGauge.get}")
              }
            }
            else {
              println("Empty activeSet")
            }
          }
          .repeat(Schedule.fixed(500.millis))
      } yield ()
    }

    managed
      .use(identity)
      .unit
  }
}
