package dev.chopsticks.dstream

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.stream.FailIfEmptyFlow.UpstreamFinishWithoutEmittingAnyItemException
import dev.chopsticks.stream.ZAkkaFlow
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import zio.clock.Clock
import zio.{RIO, Schedule, Task, UIO, URIO, URLayer, ZIO}

object DstreamMaster {
  final case class DstreamMasterConfig(
    parallelism: PosInt,
    ordered: Boolean
  )

  trait Service[In, Assignment, Result, Out] {
    def createFlow[R1, R2, R3](config: DstreamMasterConfig, createAssignment: In => URIO[R1, Assignment])(
      handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
    )(retrySchedule: Schedule[R3, Throwable, Any]): URIO[R1 with R2 with R3, Flow[In, Out, NotUsed]]
  }

  private val defaultRetrySchedule = Schedule
    .identity[Throwable]
    .whileInput[Throwable] {
      case UpstreamFinishWithoutEmittingAnyItemException => true
      case _ => false
    }

  private[dstream] def createRunnerFlowFactory[In: zio.Tag, Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag](
    config: DstreamMasterConfig
  ) = {
    for {
      akkaSvc <- ZIO.access[AkkaEnv](_.get)
      stateSvc <- ZIO.access[DstreamState[Assignment, Result]](_.get)

      flowFactoryManagedFn =
        ZRunnable {
          (
            createAssignment: In => UIO[Assignment],
            handleResult: (In, WorkResult[Result]) => Task[Out],
            retrySchedule: Schedule[Any, Throwable, Any]
          ) =>
            val process = (context: In) => {
              val task = for {
                assignment <- createAssignment(context)
                result <- {
                  ZIO.bracket(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
                    stateSvc
                      .report(assignmentId)
                      .flatMap {
                        case None => ZIO.unit
                        case Some(worker) =>
                          UIO {
                            import akkaSvc.actorSystem
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

//              val mandatoryRetryPolicy: PartialFunction[Throwable, Boolean] = {
//                case UpstreamFinishWithoutEmittingAnyItemException => true
//              }

              task
                .retry(
                  defaultRetrySchedule || retrySchedule
                )
            }

            val zflow =
              if (config.ordered) {
                ZAkkaFlow[In]
                  .interruptibleMapAsync(config.parallelism)(process)
              }
              else {
                ZAkkaFlow[In]
                  .interruptibleMapAsyncUnordered(config.parallelism)(process)
              }

            zflow.make
        }

      flowFactory <- flowFactoryManagedFn.toZIO
    } yield flowFactory
  }

  def live[In: zio.Tag, Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag]: URLayer[
    Clock with AkkaEnv with DstreamState[Assignment, Result],
    DstreamMaster[In, Assignment, Result, Out]
  ] = {
    val runnable = ZRunnable(createRunnerFlowFactory[In, Assignment, Result, Out] _)

    runnable.toLayer[Service[In, Assignment, Result, Out]] { fn =>
      new Service[In, Assignment, Result, Out] {
        override def createFlow[R1, R2, R3](
          config: DstreamMasterConfig,
          createAssignment: In => URIO[R1, Assignment]
        )(
          handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
        )(
          retrySchedule: Schedule[R3, Throwable, Any]
        ): URIO[R1 with R2 with R3, Flow[In, Out, NotUsed]] = {
          fn(config).flatMap { create =>
            for {
              env <- ZIO.environment[R1 with R2 with R3]
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
