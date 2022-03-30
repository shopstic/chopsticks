package dev.chopsticks.fdb.zio

import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbDatabase
import zio.{Fiber, RIO, ZIO, ZRef}

import scala.jdk.FutureConverters._

object ZFdbTransaction {
  def apply[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](backend: FdbDatabase[BCF, CFS])
    : ZFdbTransaction[BCF, CFS] = new ZFdbTransaction(backend)
}

final class ZFdbTransaction[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]](backend: FdbDatabase[BCF, CFS]) {
  def read[R, V](fn: ZFdbReadApi[BCF] => RIO[R, V]): RIO[R, V] = {
    for {
      innerFibRef <- ZRef.make(Option.empty[Fiber.Runtime[Throwable, V]])
      fib <- ZIO
        .runtime[R]
        .flatMap { env =>
          backend.uninterruptibleRead(api => {
            env
              .unsafeRunToFuture(
                for {
                  innerFib <- fn(new ZFdbReadApi[BCF](api)).fork
                  _ <- innerFibRef.set(Some(innerFib))
                  ret <- innerFib.join
                } yield ret
              )
              .asJava
              .toCompletableFuture
          })
        }
        .fork
      ret <- fib.join.onInterrupt(innerFibRef.get.flatMap {
        case Some(innerFib) => innerFib.interrupt
        case None => ZIO.unit
      })
    } yield ret
  }

  def write[R, V](
    fn: ZFdbWriteApi[BCF] => RIO[R, V],
    name: => String = "ZFdbTransaction"
  ): RIO[MeasuredLogging with R, V] = {
    ZIO.runtime[R].flatMap { env =>
      backend.write(
        name,
        api => {
          env.unsafeRunToFuture(fn(new ZFdbWriteApi[BCF](api))).asJava.toCompletableFuture
        }
      )
    }
  }
}
