package dev.chopsticks.fp

import zio.{Has, URLayer, ZIO, ZManaged}

final class ZRunnable[A, R, E, V] private (fn: A => ZIO[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: (A => ZIO[Any, E, V]) => S): URLayer[R, Has[S]] = {
    ZManaged
      .access[R](env => {
        val newFn = (arg: A) => fn(arg).provide(env)
        serviceFactory(newFn)
      })
      .toLayer
  }

  def toZIO: ZIO[R, E, A => ZIO[Any, E, V]] = {
    ZIO
      .access[R](env => {
        (arg: A) => fn(arg).provide(env)
      })
  }
}

object ZRunnable {
  def apply[A, R, E, V](fn: A => ZIO[R, E, V]): ZRunnable[A, R, E, V] = new ZRunnable(fn)
}
