package dev.chopsticks.dstream

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.fp.ZManageable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.stream.FailIfEmptyFlow.UpstreamFinishWithoutEmittingAnyItemException
import dev.chopsticks.stream.ZAkkaFlow
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.clock.Clock
import zio.{Exit, RIO, Schedule, Tag, Task, UIO, URIO, URLayer, URManaged, ZIO, ZManaged}

object DstreamMaster {
  final case class DstreamMasterConfig(
    serviceId: NonEmptyString,
    parallelism: PosInt,
    ordered: Boolean
  )

  trait Service[In, Assignment, Result, Out] {
    def manageFlow[R1, R2, R3](config: DstreamMasterConfig, createAssignment: In => URIO[R1, Assignment])(
      handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
    )(retrySchedule: Schedule[R3, Throwable, Any]): URManaged[R1 with R2 with R3, Flow[In, Out, NotUsed]]
  }

  private val defaultRetrySchedule = Schedule
    .identity[Throwable]
    .whileInput[Throwable] {
      case UpstreamFinishWithoutEmittingAnyItemException => true
      case _ => false
    }

  private[dstream] def createRunnerFlowFactory[In: Tag, Assignment: Tag, Result: Tag, Out: Tag](
    config: DstreamMasterConfig
  ) = {
    def createFlow(
      createAssignment: In => UIO[Assignment],
      handleResult: (In, WorkResult[Result]) => Task[Out],
      retrySchedule: Schedule[Any, Throwable, Any]
    ) = {
      for {
        metrics <- ZManaged.accessManaged[DstreamMasterMetricsManager](_.get.manage(config.serviceId.value))
        stateSvc <- ZManaged.access[DstreamState[Assignment, Result]](_.get)
        process = (context: In) => {
          val attempt = ZIO
            .bracketExit {
              UIO(metrics.attemptsTotal.inc())
            } {
              (_: Unit, exit: Exit[Throwable, Out]) =>
                UIO {
                  if (exit.succeeded) {
                    metrics.successesTotal.inc()
                  }
                  else {
                    metrics.failuresTotal.inc()
                  }
                }
            } { (_: Unit) =>
              for {
                assignment <- createAssignment(context)
                result <- {
                  ZIO.bracket(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
                    stateSvc
                      .report(assignmentId)
                      .flatMap {
                        case None => ZIO.unit
                        case Some(worker) =>
                          AkkaEnv.actorSystem.map { implicit as =>
                            worker.source.runWith(Sink.cancelled)
                          }.ignore
                      }
                      .orDie
                  } { assignmentId =>
                    stateSvc
                      .awaitForWorker(assignmentId)
                      .flatMap(result => handleResult(context, result))
                  }
                }
              } yield result
            }

          UIO(metrics.assignmentsTotal.inc()) *> attempt.retry(
            defaultRetrySchedule || retrySchedule
          )
        }

        zflow =
          if (config.ordered) {
            ZAkkaFlow[In]
              .interruptibleMapAsync(config.parallelism)(process)
          }
          else {
            ZAkkaFlow[In]
              .interruptibleMapAsyncUnordered(config.parallelism)(process)
          }

        flow <- zflow.make.toManaged_
      } yield flow
    }

    val manageable = ZManageable(createFlow _)

    manageable.toZManaged
  }

  def live[In: Tag, Assignment: Tag, Result: Tag, Out: Tag]: URLayer[
    AkkaEnv with Clock with DstreamState[Assignment, Result] with DstreamMasterMetricsManager,
    DstreamMaster[In, Assignment, Result, Out]
  ] = {
    val manageable = ZManageable(createRunnerFlowFactory[In, Assignment, Result, Out] _)

    manageable.toLayer[Service[In, Assignment, Result, Out]] { fn =>
      new Service[In, Assignment, Result, Out] {
        override def manageFlow[R1, R2, R3](
          config: DstreamMasterConfig,
          createAssignment: In => URIO[R1, Assignment]
        )(
          handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
        )(
          retrySchedule: Schedule[R3, Throwable, Any]
        ): URManaged[R1 with R2 with R3, Flow[In, Out, NotUsed]] = {
          fn(config).flatMap { create =>
            for {
              env <- ZManaged.environment[R1 with R2 with R3]
              flow <- create(
                in => createAssignment(in).provide(env),
                (in, workResult) => handleResult(in, workResult).provide(env),
                retrySchedule.provide(env)
              )
            } yield flow
          }
        }
      }
    }
  }
}
