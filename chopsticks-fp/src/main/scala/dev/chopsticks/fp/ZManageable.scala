package dev.chopsticks.fp

import zio.{Has, URLayer, ZManaged}

final class ZManageable1[A, R, E, V](fn: A => ZManaged[R, E, V]) {
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

final class ZManageable2[A1, A2, R, E, V](fn: (A1, A2) => ZManaged[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: ((A1, A2) => ZManaged[Any, E, V]) => S): URLayer[R, Has[S]] = {
    ZManaged
      .access[R](env => {
        val newFn = (arg1: A1, arg2: A2) => fn(arg1, arg2).provide(env)
        serviceFactory(newFn)
      })
      .toLayer
  }

  def toZManaged: ZManaged[R, E, (A1, A2) => ZManaged[Any, E, V]] = {
    ZManaged
      .access[R](env => {
        (arg1: A1, arg2: A2) => fn(arg1, arg2).provide(env)
      })
  }
}

object ZManageable {
  def apply[A, R, E, V](fn: A => ZManaged[R, E, V]): ZManageable1[A, R, E, V] = new ZManageable1(fn)
  def apply[A1, A2, R, E, V](fn: (A1, A2) => ZManaged[R, E, V]): ZManageable2[A1, A2, R, E, V] = new ZManageable2(fn)
}
