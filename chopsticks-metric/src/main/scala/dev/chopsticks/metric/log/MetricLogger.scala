package dev.chopsticks.metric.log

import dev.chopsticks.metric.log.MetricLogger.PeriodicValue
import dev.chopsticks.metric.{MetricCounter, MetricGauge, MetricReference}
import zio.{LogAnnotation, Ref, Schedule, UIO, ULayer, URIO, ZIO, ZLayer}

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.ListMap
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

trait MetricLogger {
  def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]]): URIO[R, Unit]
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

  def snapshot[A: Numeric](value: A): PeriodicSnapshot = PeriodicSnapshot(value.toString)
  def snapshot(value: AtomicReference[LocalDate]): PeriodicSnapshot = {
    val dt = value.get
    PeriodicSnapshot(if (dt == LocalDate.MIN) "never" else dt.toString)
  }

  def referenceSnapshot[A, B](
    metricProviders: Iterable[A]
  )(extractCounter: A => MetricReference[B])(render: Option[B] => String)(implicit
    ordering: Ordering[B]
  ): PeriodicSnapshot =
    PeriodicSnapshot {
      render {
        metricProviders
          .iterator
          .map(provider => extractCounter(provider).get)
          .collect { case Some(value) => value }
          .maxOption
      }
    }

  def sum[A, M: MetricValueExtractor](metricProviders: Iterable[A])(extractCounter: A => M): PeriodicSnapshot =
    val value = metricProviders.iterator.map(p => MetricValueExtractor.extract(extractCounter(p))).sum
    snapshot(value)

  def sumMulti[A, M: MetricValueExtractor](metricProviders: Iterable[A])(extractCounters: A => Iterable[M])
    : PeriodicSnapshot =
    val value = metricProviders.iterator.flatMap(extractCounters).map(m => MetricValueExtractor.extract(m)).sum
    snapshot(value)

  def sumRate[A](metricProviders: Iterable[A])(extractCounter: A => MetricCounter): PeriodicRate =
    val values =
      metricProviders
        .iterator
        .map(provider => provider -> extractCounter(provider).get)
        .toMap[Any, Double]
    PeriodicRate(values)

  def sumRateMulti[M](metricProviders: Iterable[M])(extractCounter: M => Iterable[MetricCounter]): PeriodicRate =
    PeriodicRate(
      metricProviders
        .view
        .map { m =>
          m -> extractCounter(m).foldLeft(0.0)(_ + _.get)
        }
        .toMap
    )

  def get: URIO[MetricLogger, MetricLogger] = ZIO.service[MetricLogger]

  def noop: ULayer[MetricLogger] = {
    ZLayer.succeed {
      new MetricLogger {
        override def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]]): URIO[R, Unit] =
          ZIO.never.unit
      }
    }
  }

  def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]]): URIO[R with MetricLogger, Unit] =
    ZIO.service[MetricLogger].flatMap(_.periodicallyCollect(collect))

  def live(
    interval: FiniteDuration = 1.second,
    log: ListMap[String, String] => UIO[Unit] = defaultLog
  ): ULayer[MetricLogger] = {
    val io =
      for {
        priorSnapRef <- Ref.make(ListMap.empty[String, PeriodicValue])
      } yield new MetricLogger {
        override def periodicallyCollect[R](collect: URIO[R, ListMap[String, PeriodicValue]]): URIO[R, Unit] = {
          val collectAndAccumulate = for {
            snapshot <- collect
            output <- priorSnapRef.modify { priorSnapshot =>
              val next = snapshot.map {
                case (k, PeriodicSnapshot(v)) =>
                  (k, v)

                case (k, PeriodicRate(values)) =>
                  val rate = values.foldLeft(0d) {
                    case (acc, (mk, mv)) =>
                      val priorValue = priorSnapshot.get(k) match {
                        case Some(PeriodicRate(priorValues)) => priorValues.getOrElse(mk, 0d)
                        case _ => 0d
                      }
                      acc + mv - priorValue
                  }
                  (k, rate.toString)
              }
              (next, snapshot)
            }
          } yield output
          collectAndAccumulate
            .tap(log)
            .repeat(Schedule.spaced(interval.toJava))
            .unit
        }
      }
    ZLayer.fromZIO(io)
  }

  private def defaultLog(pairs: ListMap[String, String]): UIO[Unit] =
    ZIO
      .logAnnotate(pairs.iterator.map { case (k, v) => LogAnnotation(k, v) }.toSet)
      .apply(ZIO.logInfo(""))

}
