package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.MetricTestApp.AppMetrics.AppMetric
import zio._

import scala.concurrent.duration._
import scala.util.Random

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

  object AppMetricsFactory extends MetricServiceFactory[AppMetric, String, AppMetrics] {
    override def create(registry: MetricRegistry.Service[AppMetric], config: String): AppMetrics = {
      import AppMetrics._
      val labels = LabelValues.of(testLabel -> config)

      new AppMetrics {
        override val fooCounter: MetricCounter = registry.counterWithLabels(fooTotal, labels)
        override val barGauge: MetricGauge = registry.gaugeWithLabels(barCurrent, labels)
      }
    }
  }

  type AppMetricsManager = MetricServiceManager[String, AppMetrics]

  object AppMetricsManager {
    def live: RLayer[MetricRegistryFactory[AppMetric], MetricServiceManager[String, AppMetrics]] =
      MetricServiceManager.live(AppMetricsFactory)
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
    } yield {
      for {
        _ <-
          metricsManager
            .manage("testing")
            .use { metrics =>
              UIO {
                metrics.fooCounter.inc()
                metrics.barGauge.set(Random.nextInt())
              } *> ZIO.unit.delay(1.second)
            }
            .flatMap(_ => ZIO.unit.delay(1.second))
            .repeat(Schedule.forever)
            .fork
        _ <- metricsManager
          .activeSet
          .map { activeSet =>
            if (activeSet.nonEmpty) {
              activeSet.foreach { m =>
                println(s"foo = ${m.fooCounter.get}")
                println(s"bar = ${m.barGauge.get}")
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
