package dev.chopsticks.sample.app

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.MetricTestApp.AppMetrics.AppMetric
import zio._

object MetricTestApp extends AkkaDiApp[Unit] {
  trait AppMetrics {
    def fooCounter: MetricCounter
    def barGauge: MetricGauge
  }

  object AppMetrics {
    object testLabel extends MetricLabel
    sealed trait AppMetric extends MetricGroup
    case object fooTotal extends CounterConfig(LabelNames of testLabel) with AppMetric
    case object barCurrent extends GaugeConfig(LabelNames of testLabel) with AppMetric
  }

  type AppMetricsManager = Has[AppMetricsManager.Service]

  object AppMetricsManager {
    trait Service extends MetricServiceTracker[AppMetrics] {
      def manage(test: String): UManaged[AppMetrics]
    }

    final case class LiveService(registryLayer: ULayer[MetricRegistry[AppMetric]])
        extends MetricServiceManager[AppMetric, AppMetrics](registryLayer)
        with Service {
      import AppMetrics._

      def manage(test: String): UManaged[AppMetrics] = {
        val labels = LabelValues.of(testLabel -> test)
        create(test) { registry =>
          new AppMetrics {
            override val fooCounter: MetricCounter = registry.counterWithLabels(fooTotal, labels)
            override val barGauge: MetricGauge = registry.gaugeWithLabels(barCurrent, labels)
          }
        }
      }
    }

    def live: URLayer[MetricRegistryFactory[AppMetric], AppMetricsManager] = {
      ZManaged.access[MetricRegistryFactory[AppMetric]](_.get)
        .map(LiveService.apply)
        .toLayer
    }
  }

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(akkaAppDi: DiModule, appConfig: Unit, allConfig: Config): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          ZLayer.succeed(PromMetricRegistry.live[AppMetric]("test")),
          AppMetricsManager.live,
          AppLayer(app)
        )
      )
    }
  }

  def app = {
    val managed = for {
      metricsManager <- ZManaged.access[AppMetricsManager](_.get)
      metrics <- metricsManager.manage("testing")
    } yield {
      println("HERE YO!")
      metrics.fooCounter.inc()
      metrics.barGauge.set(100)

      Task {
        metricsManager.activeSet.foreach { m =>
          println(s"foo = ${m.fooCounter.get}")
          println(s"bar = ${m.barGauge.get}")
        }
      }
        .repeat(Schedule.fixed(zio.duration.Duration(1, TimeUnit.SECONDS)))
    }

    managed
      .use(identity)
      .unit
  }
}
