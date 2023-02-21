package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbDatabase
import zio.*

import scala.jdk.FutureConverters._

object ZFdbTransaction {
  def apply[CFS <: ColumnFamily[_, _]](backend: FdbDatabase[CFS]): ZFdbTransaction[CFS] =
    new ZFdbTransaction(backend)
}

final class ZFdbTransaction[CFS <: ColumnFamily[_, _]](backend: FdbDatabase[CFS]) {
  def read[R, V](fn: ZFdbReadApi[CFS] => RIO[R, V]): RIO[R, V] = {
    for {
      innerFibRef <- Ref.make(Option.empty[Fiber.Runtime[Throwable, V]])
      fib <- ZIO
        .runtime[R]
        .flatMap { env =>
          backend.uninterruptibleRead(api => {
            Unsafe.unsafe { implicit unsafe =>
              env
                .unsafe
                .runToFuture(
                  for {
                    innerFib <- fn(new ZFdbReadApi[CFS](api)).fork
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
    fn: ZFdbWriteApi[CFS] => RIO[R, V],
    name: => String = "ZFdbTransaction"
  ): RIO[R, V] = {
    ZIO.runtime[R].flatMap { env =>
      backend.write(
        name,
        api => {
          Unsafe.unsafe { implicit unsafe =>
            env.unsafe.runToFuture(fn(new ZFdbWriteApi[CFS](api))).asJava.toCompletableFuture
          }
        }
      )
    }
  }
}
