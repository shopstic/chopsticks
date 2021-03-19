package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.Http
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.{ZManageable, ZRunnable}
import dev.chopsticks.stream.FailIfEmptyFlow.UpstreamFinishWithoutEmittingAnyItemException
import dev.chopsticks.stream.ZAkkaFlow
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.{RIO, RManaged, Task, UIO, URIO, URLayer, ZIO, ZManaged}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DstreamServerRunner {
  final case class DstreamServerConfig(
    port: PortNumber,
    parallelism: PosInt,
    ordered: Boolean,
    interface: NonEmptyString = "0.0.0.0",
    shutdownTimeout: Timeout = 10.seconds
  )

  trait Service[In, Assignment, Result, Out] {
    def manage[R1, R2](config: DstreamServerConfig, createAssignment: In => URIO[R1, Assignment])(
      makeResultHandler: (In, WorkResult[Result]) => RIO[R2, Out]
    ): RManaged[R1 with R2, (Http.ServerBinding, Flow[In, Out, NotUsed])]
  }

  private[dstream] def manageServer[In: zio.Tag, Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag](
    config: DstreamServerConfig
  ) = {
    for {
      akkaSvc <- ZManaged.access[AkkaEnv](_.get)
      akkaRuntime <- ZManaged.runtime[AkkaEnv]
      stateSvc <- ZManaged.access[DstreamState[Assignment, Result]](_.get)
      handlerFactory <- ZManaged.access[DstreamServerApi[Assignment, Result]](_.get)
      handler <- handlerFactory
        .create { (in: Source[Result, NotUsed], metadata: Metadata) =>
          import akkaSvc.{actorSystem, dispatcher}

          val (ks, inSource) = in
            .viaMat(KillSwitches.single)(Keep.right)
            .preMaterialize()

          val futureSource = akkaRuntime.unsafeRunToFuture(stateSvc.enqueueWorker(inSource, metadata))

          Source
            .futureSource(futureSource)
            .watchTermination() {
              case (_, f) =>
                f
                  .transformWith { result =>
                    futureSource
                      .cancel()
                      .map(exit => result.flatMap(_ => exit.toEither.toTry))
                  }
                  .onComplete {
                    case Success(_) => ks.shutdown()
                    case Failure(ex) => ks.abort(ex)
                  }
                NotUsed
            }
        }
        .toManaged_

      binding <- ZManaged
        .make {
          for {
            binding <- Task
              .fromFuture { _ =>
                import akkaSvc.actorSystem
                val settings = ServerSettings(actorSystem)

                Http()
                  .newServerAt(interface = config.interface, port = config.port)
                  .withSettings(
                    settings
                      .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
                  )
                  .bind(handler)
              }
          } yield binding
        } { binding =>
          Task
            .fromFuture(_ => binding.terminate(config.shutdownTimeout.duration))
            .orDie
        }

      flowFactoryManagedFn =
        ZRunnable { (createAssignment: In => UIO[Assignment], handleResult: (In, WorkResult[Result]) => Task[Out]) =>
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

            task.retryWhile {
              case UpstreamFinishWithoutEmittingAnyItemException => true
              case _ => false
            }
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

      flowFactory <- flowFactoryManagedFn.toZIO.toManaged_
    } yield binding -> flowFactory
  }

  def live[In: zio.Tag, Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag]: URLayer[
    DstreamServerApi[Assignment, Result] with DstreamState[Assignment, Result] with AkkaEnv,
    DstreamServerRunner[In, Assignment, Result, Out]
  ] = {

    ZManageable(manageServer[In, Assignment, Result, Out] _)
      .toLayer[Service[In, Assignment, Result, Out]](fn =>
        new Service[In, Assignment, Result, Out] {
          override def manage[R1, R2](config: DstreamServerConfig, createAssignment: In => URIO[R1, Assignment])(
            handleResult: (In, WorkResult[Result]) => RIO[R2, Out]
          ): ZManaged[R1 with R2, Throwable, (Http.ServerBinding, Flow[In, Out, NotUsed])] = {
            fn(config).flatMap { case (binding, createFlow) =>
              for {
                env <- ZManaged.environment[R1 with R2]
                flow <- createFlow(
                  in => createAssignment(in).provide(env),
                  (in, workResult) => handleResult(in, workResult).provide(env)
                )
                  .toManaged_
              } yield binding -> flow
            }
          }
        }
      )
  }
}
