package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.metric.DstreamMasterMetricsManager
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.stream.FailIfEmptyFlow.UpstreamFinishWithoutEmittingAnyItemException
import dev.chopsticks.stream.ZAkkaFlow
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.{Exit, RIO, Schedule, Scope, Tag, Task, URIO, URLayer, ZIO, ZLayer}

trait DstreamMaster[In, Assignment, Result, Out] {
  def manageFlow[R1, R2, R3](config: DstreamMaster.DstreamMasterConfig, createAssignment: In => URIO[R1, Assignment])(
    handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
  )(withAttempt: (In, Task[Out]) => RIO[R3, Out])
    : URIO[R1 with R2 with R3 with Scope, ZAkkaFlow[Any, Nothing, In, Out, NotUsed]]
}

object DstreamMaster {
  final case class DstreamMasterConfig(
    serviceId: NonEmptyString,
    parallelism: PosInt,
    ordered: Boolean
  )

  val defaultRetrySchedule: Schedule[Any, Throwable, Throwable] = Schedule
    .identity[Throwable]
    .whileInput[Throwable] {
      case UpstreamFinishWithoutEmittingAnyItemException => true
      case _ => false
    }

  def live[In: Tag, Assignment: Tag, Result: Tag, Out: Tag]: URLayer[
    PekkoEnv with DstreamState[Assignment, Result] with DstreamMasterMetricsManager,
    DstreamMaster[In, Assignment, Result, Out]
  ] = {
    val effect =
      for {
        metricsManager <- ZIO.service[DstreamMasterMetricsManager]
        stateSvc <- ZIO.service[DstreamState[Assignment, Result]]
        pekkoEnv <- ZIO.environment[PekkoEnv]
      } yield new DstreamMaster[In, Assignment, Result, Out] {
        override def manageFlow[R1, R2, R3](
          config: DstreamMasterConfig,
          createAssignment: In => URIO[R1, Assignment]
        )(handleResult: (In, WorkResult[Result]) => RIO[R2, Out])(withAttempt: (In, Task[Out]) => RIO[R3, Out])
          : URIO[R1 with R2 with R3 with Scope, ZAkkaFlow[Any, Nothing, In, Out, NotUsed]] = {
          val manageFlowEffect = for {
            metrics <- metricsManager.manage(config.serviceId.value)
            // todo [migration] check it
            rEnv <- ZIO.environment[R1 with R2 with R3]
//            rEnv <- ZIO.environment[R1 with R2 with R3 with Scope]
            process = (context: In) => {
              val attempt = ZIO
                .acquireReleaseExitWith {
                  ZIO.succeed(metrics.attemptsTotal.inc())
                } {
                  (_: Unit, exit: Exit[Throwable, Out]) =>
                    ZIO.succeed {
                      if (exit.isSuccess) {
                        metrics.successesTotal.inc()
                      }
                      else {
                        metrics.failuresTotal.inc()
                      }
                    }
                } { (_: Unit) =>
                  for {
                    assignment <- createAssignment(context).provideEnvironment(rEnv)
                    result <- {
                      ZIO.acquireReleaseWith(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
                        stateSvc
                          .report(assignmentId)
                          .flatMap {
                            case None => ZIO.unit
                            case Some(worker) =>
                              PekkoEnv.actorSystem
                                .map { implicit as => worker.source.runWith(Sink.cancelled) }
                                .ignore
                          }
                          .orDie
                      } { assignmentId =>
                        stateSvc
                          .awaitForWorker(assignmentId)
                          .flatMap(result => handleResult(context, result).provideEnvironment(rEnv))
                      }
                    }
                  } yield result
                }

              ZIO.succeed(metrics.assignmentsTotal.inc()) *>
                withAttempt(context, attempt.provideEnvironment(pekkoEnv))
                  .provideEnvironment(rEnv)
            }
            zflow =
              if (config.ordered) {
                ZAkkaFlow[In]
                  .mapAsync(config.parallelism)(process)
              }
              else {
                ZAkkaFlow[In]
                  .mapAsyncUnordered(config.parallelism)(process)
              }
            zFlowWithoutEnv <- zflow.toZIO.provideEnvironment(pekkoEnv)
          } yield zFlowWithoutEnv

          manageFlowEffect
        }
      }: DstreamMaster[In, Assignment, Result, Out]

    ZLayer(effect)

//    val manageable = ZManageable(createRunnerFlowFactory[In, Assignment, Result, Out] _)
//
//    manageable.toLayer[Service[In, Assignment, Result, Out]] { fn =>
//      new Service[In, Assignment, Result, Out] {
//        override def manageFlow[R1, R2, R3](
//          config: DstreamMasterConfig,
//          createAssignment: In => URIO[R1, Assignment]
//        )(
//          handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
//        )(
//          withAttempt: (In, Task[Out]) => RIO[R3, Out]
//        ): URManaged[R1 with R2 with R3, ZAkkaFlow[Any, Nothing, In, Out, NotUsed]] = {
//          fn(config).flatMap { create =>
//            for {
//              env <- ZManaged.environment[R1 with R2 with R3]
//              flow <- create(
//                in => createAssignment(in).provide(env),
//                (in, workResult) => handleResult(in, workResult).provide(env),
//                (in, task) => withAttempt(in, task).provide(env)
//              )
//            } yield flow
//          }
//        }
//      }
//    }
  }

//  private[dstream] def createRunnerFlowFactory[In, Assignment: Tag, Result: Tag, Out](
//    config: DstreamMasterConfig
//  ) = {
//    def createFlow(
//      createAssignment: In => UIO[Assignment],
//      handleResult: (In, WorkResult[Result]) => Task[Out],
//      withAttempt: (In, Task[Out]) => Task[Out]
//    ) = {
//      for {
//        metrics <- ZIO.serviceWithZIO[DstreamMasterMetricsManager](_.manage(config.serviceId.value))
//        stateSvc <- ZIO.service[DstreamState[Assignment, Result]]
//        pekkoEnv <- ZIO.environment[PekkoEnv]
//        process = (context: In) => {
//          val attempt = ZIO
//            .acquireReleaseExitWith {
//              ZIO.succeed(metrics.attemptsTotal.inc())
//            } {
//              (_: Unit, exit: Exit[Throwable, Out]) =>
//                ZIO.succeed {
//                  if (exit.isSuccess) {
//                    metrics.successesTotal.inc()
//                  }
//                  else {
//                    metrics.failuresTotal.inc()
//                  }
//                }
//            } { (_: Unit) =>
//              for {
//                assignment <- createAssignment(context)
//                result <- {
//                  ZIO.acquireReleaseWith(stateSvc.enqueueAssignment(assignment)) { assignmentId =>
//                    stateSvc
//                      .report(assignmentId)
//                      .flatMap {
//                        case None => ZIO.unit
//                        case Some(worker) =>
//                          PekkoEnv.actorSystem.map { implicit as =>
//                            worker.source.runWith(Sink.cancelled)
//                          }.ignore
//                      }
//                      .orDie
//                  } { assignmentId =>
//                    stateSvc
//                      .awaitForWorker(assignmentId)
//                      .flatMap(result => handleResult(context, result))
//                  }
//                }
//              } yield result
//            }
//
//          ZIO.succeed(metrics.assignmentsTotal.inc()) *> withAttempt(context, attempt.provideEnvironment(pekkoEnv))
//        }
//
//        zflow =
//          if (config.ordered) {
//            ZAkkaFlow[In]
//              .mapAsync(config.parallelism)(process)
//          }
//          else {
//            ZAkkaFlow[In]
//              .mapAsyncUnordered(config.parallelism)(process)
//          }
//        zFlowWithoutEnv <- zflow.toZIO
//      } yield zFlowWithoutEnv
//    }
//
//    val manageable = ZManageable(createFlow _)
//
//    manageable.toZManaged
//  }
}
