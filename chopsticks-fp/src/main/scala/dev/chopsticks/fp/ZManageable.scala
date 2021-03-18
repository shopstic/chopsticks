package dev.chopsticks.fp

import zio.{Has, URLayer, ZManaged}

final class ZManageable[A, R, E, V] private (fn: A => ZManaged[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: (A => ZManaged[Any, E, V]) => S): URLayer[R, Has[S]] = {
    ZManaged
      .access[R](env => {
        val newFn = (arg: A) => fn(arg).provide(env)
        serviceFactory(newFn)
      })
      .toLayer
  }

  def toManaged[S: zio.Tag]: ZManaged[R, E, A => ZManaged[Any, E, V]] = {
    ZManaged
      .access[R](env => {
        (arg: A) => fn(arg).provide(env)
      })
  }
}

object ZManageable {
  def apply[A, R, E, V](fn: A => ZManaged[R, E, V]): ZManageable[A, R, E, V] = new ZManageable(fn)
}
