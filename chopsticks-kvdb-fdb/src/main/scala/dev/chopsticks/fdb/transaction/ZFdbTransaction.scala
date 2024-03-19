package dev.chopsticks.fdb.transaction

import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbDatabase
import zio.{Fiber, RIO, Ref, Unsafe, ZIO}

import scala.jdk.FutureConverters._

object ZFdbTransaction {
  def apply[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](backend: FdbDatabase[BCF, CFS])
    : ZFdbTransaction[BCF, CFS] = new ZFdbTransaction(backend)
}

final class ZFdbTransaction[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]](backend: FdbDatabase[BCF, CFS]) {
  def read[R, V](fn: ZFdbReadApi[BCF] => RIO[R, V]): RIO[R, V] = {
    for {
      innerFibRef <- Ref.make(Option.empty[Fiber.Runtime[Throwable, V]])
      fib <- ZIO
        .runtime[R]
        .flatMap { rt =>
          backend.uninterruptibleRead(api => {
            Unsafe.unsafe { implicit unsafe =>
              rt
                .unsafe.runToFuture(
                  for {
                    innerFib <- fn(new ZFdbReadApi[BCF](api)).fork
                    _ <- innerFibRef.set(Some(innerFib))
                    ret <- innerFib.join
                  } yield ret
                )
                .asJava
                .toCompletableFuture
            }
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
  ): RIO[IzLogging with R, V] = {
    ZIO.runtime[R].flatMap { rt =>
      backend.write(
        name,
        api => {
          Unsafe.unsafe { implicit unsafe =>
            rt.unsafe.runToFuture(fn(new ZFdbWriteApi[BCF](api))).asJava.toCompletableFuture
          }
        }
      )
    }
  }
}
