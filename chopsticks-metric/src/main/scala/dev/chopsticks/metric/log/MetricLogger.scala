package dev.chopsticks.metric.log

import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.metric.{MetricCounter, MetricGauge, MetricReference}
import dev.chopsticks.metric.log.MetricLogger.PeriodicValue
import izumi.logstage.api.IzLogger
import izumi.logstage.api.Log.{CustomContext, LogArg}
import izumi.logstage.api.rendering.{AnyEncoded, StrictEncoded}
import zio.{Ref, Schedule, UIO, ULayer, URIO, URLayer, ZIO, ZLayer}

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

trait MetricLogger {
  def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]])(implicit logCtx: LogCtx): URIO[R, Unit]
}

object MetricLogger {
  sealed trait PeriodicValue extends Product with Serializable
  final case class PeriodicSnapshot(value: String) extends PeriodicValue
  final case class PeriodicRate(values: Map[Any, Double]) extends PeriodicValue

  trait MetricValueExtractor[M] {
    def get(metric: M): Double
  }
  object MetricValueExtractor {
    def extract[M: MetricValueExtractor](metric: M): Double = implicitly[MetricValueExtractor[M]].get(metric)

    implicit val metricGaugeValueExtractor: MetricValueExtractor[MetricGauge] = _.get
    implicit val metricCounterValueExtractor: MetricValueExtractor[MetricCounter] = _.get
  }

  def snapshot(value: String): PeriodicSnapshot = PeriodicSnapshot(value)

  @nowarn("cat=unused")
  def snapshot[A: Numeric](value: A): PeriodicSnapshot = PeriodicSnapshot(value.toString)
  def snapshot(value: AtomicReference[LocalDate]): PeriodicSnapshot = {
    val dt = value.get
    PeriodicSnapshot(if (dt == LocalDate.MIN) "never" else dt.toString)
  }

  def referenceSnapshot[A, B](
    metricProviders: Iterable[A]
  )(extractCounter: A => MetricReference[B])(render: Option[B] => String)(implicit
    ordering: Ordering[B]
  ): PeriodicSnapshot = {
    PeriodicSnapshot {
      render {
        metricProviders
          .iterator
          .map(provider => extractCounter(provider).get)
          .collect { case Some(value) => value }
          .maxOption
      }
    }
  }

  def sum[A, M: MetricValueExtractor](metricProviders: Iterable[A])(extractCounter: A => M): PeriodicSnapshot = {
    val value = metricProviders.iterator.map(p => MetricValueExtractor.extract(extractCounter(p))).sum
    snapshot(value)
  }

  def sumMulti[A, M: MetricValueExtractor](metricProviders: Iterable[A])(extractCounters: A => Iterable[M])
    : PeriodicSnapshot = {
    val value = metricProviders.iterator.flatMap(extractCounters).map(m => MetricValueExtractor.extract(m)).sum
    snapshot(value)
  }

  def sumRate[A](metricProviders: Iterable[A])(extractCounter: A => MetricCounter): PeriodicRate = {
    val values =
      metricProviders
        .iterator
        .map(provider => provider -> extractCounter(provider).get)
        .toMap[Any, Double]

    PeriodicRate(values)
  }

  def sumRateMulti[M](metricProviders: Iterable[M])(extractCounter: M => Iterable[MetricCounter]): PeriodicRate = {
    PeriodicRate(
      metricProviders
        .view
        .map { m =>
          m -> extractCounter(m).foldLeft(0.0)(_ + _.get)
        }
        .toMap
    )
  }

  def get: URIO[MetricLogger, MetricLogger] = ZIO.service[MetricLogger]

  def noop: ULayer[MetricLogger] = {
    ZLayer.succeed {
      new MetricLogger {
        override def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]])(implicit
          logCtx: LogCtx
        ): URIO[R, Unit] = {
          ZIO.never.unit
        }
      }
    }
  }

  def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]])(implicit
    logCtx: LogCtx
  ): URIO[R with MetricLogger, Unit] = {
    ZIO.service[MetricLogger].flatMap(_.periodicallyCollect(collect))
  }

  def live(
    interval: FiniteDuration = 1.second,
    log: (IzLogger, ListMap[String, AnyEncoded]) => UIO[Unit] = defaultLog
  ): URLayer[IzLogging, MetricLogger] = {
    val effect =
      for {
        izLogging <- ZIO.service[IzLogging]
      } yield new MetricLogger {
        override def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]])(implicit
          logCtx: LogCtx
        ) = {
          for {
            priorSnapRef <- Ref.make(ListMap.empty[String, PeriodicValue])
            logger = izLogging.loggerWithCtx(logCtx)
            collectAndAccumulate = {
              for {
                snapshot <- collect
                output <- priorSnapRef.modify { priorSnapshot =>
                  val next = snapshot.map {
                    case (k, PeriodicSnapshot(v)) =>
                      (k, StrictEncoded.to(v))

                    case (k, PeriodicRate(values)) =>
                      val rate = values.foldLeft(0d) {
                        case (acc, (mk, mv)) =>
                          val priorValue = priorSnapshot.get(k) match {
                            case Some(PeriodicRate(priorValues)) => priorValues.getOrElse(mk, 0d)
                            case _ => 0d
                          }
                          acc + mv - priorValue
                      }
                      (k, StrictEncoded.to(rate))
                  }
                  (next, snapshot)
                }
              } yield output
            }
            _ <- {
              collectAndAccumulate
                .tap(log(logger, _))
                .repeat(Schedule.spaced(interval.toJava))
                .unit
            }
          } yield ()
        }
      }

    ZLayer(effect)
  }

  private def defaultLog(logger: IzLogger, pairs: ListMap[String, AnyEncoded]): UIO[Unit] = {
    ZIO.succeed {
      val logArgs = pairs.map { case (k, v) =>
        LogArg(Seq(k), v.value, hiddenName = false, v.codec)
      }.toList

      logger(CustomContext(logArgs))
        .info("")
    }
  }

}
