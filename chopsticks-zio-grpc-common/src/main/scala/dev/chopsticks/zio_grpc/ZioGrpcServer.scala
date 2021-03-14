package dev.chopsticks.zio_grpc

import akka.util.Timeout
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import scalapb.zio_grpc.ZBindableService
import zio.blocking.{effectBlocking, Blocking}
import zio.clock.Clock
import zio._

import java.net.InetSocketAddress

object ZioGrpcServer {
  trait Config {
    def interface: NonEmptyString

    def port: PortNumber

    def shutdownTimeout: Timeout
  }

  trait Binding {
    def interface: NonEmptyString
    def port: PortNumber
  }

  trait Service[B <: Binding] {
    def manage[R <: Has[_]: zio.Tag](config: Config)(makeServiceDef: RIO[R, ServerServiceDefinition]): RManaged[R, B]
  }

  def live[B <: Binding: zio.Tag](
    makeBinding: (NonEmptyString, PortNumber) => B
  ): URLayer[Blocking with Clock, ZioGrpcServer[B]] = {
    val managedService = for {
      env <- ZManaged.environment[Blocking with Clock]
    } yield {
      val service: Service[B] = new Service[B] {
        def manage[R <: Has[_]: zio.Tag](config: Config)(makeServiceDef: RIO[R, ServerServiceDefinition])
          : RManaged[R, B] = {
          val managed: RManaged[Blocking with Clock with R, B] = for {
            healthServiceDef <- ZBindableService.serviceDefinition(new HealthGrpcServiceImpl()).toManaged_
            server <- ZManaged.fromEffect {
              ZIO.effectSuspend {
                val bindInterface = config.interface.value
                val desiredPort = config.port.value

                val builder =
                  if (bindInterface == "0.0.0.0") {
                    NettyServerBuilder
                      .forPort(desiredPort)
                  }
                  else {
                    NettyServerBuilder
                      .forAddress(new InetSocketAddress(bindInterface, desiredPort))
                  }

                for {
                  serviceDef <- makeServiceDef
                  server <- Task {
                    builder
                      .addService(ProtoReflectionService.newInstance())
                      .addService(healthServiceDef)
                      .addService(serviceDef)
                      .build()
                  }
                } yield server
              }
            }
            _ <- ZManaged
              .make {
                effectBlocking(server.start).as(server)
              } { server =>
                (Task(server.shutdown()) *> effectBlocking {
                  val shutdownTimeout = config.shutdownTimeout.duration
                  server.awaitTermination(shutdownTimeout.length, shutdownTimeout.unit)
                }).ignore
              }
            effectivePort <- Task(server.getPort).toManaged_
          } yield makeBinding(config.interface, PortNumber.unsafeFrom(effectivePort))

          managed.provideSome[R](env ++ [R] _)
        }
      }

      service
    }

    managedService.toLayer
  }
}
