package dev.chopsticks.fp

import zio.{Scope, URIO, URLayer, ZEnvironment, ZIO, ZLayer}

final class ZRunnable1[A, R, E, V](fn: A => ZIO[R with Scope, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: (A => ZIO[Any, E, V]) => S): URLayer[R, S] = {
    val io: URIO[R with Scope, S] = ZIO
      .environmentWith[R with Scope]((env: ZEnvironment[R with Scope]) => {
        val newFn = (arg: A) => fn(arg).provideEnvironment(env)
        serviceFactory(newFn)
      })
    ZLayer.scoped[R](io)
  }
}

final class ZRunnable2[A1, A2, R, E, V](fn: (A1, A2) => ZIO[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: ((A1, A2) => ZIO[Any, E, V]) => S): URLayer[R, S] = {
    val io = ZIO
      .environmentWith[R with Scope](env => {
        val newFn = (arg1: A1, arg2: A2) => fn(arg1, arg2).provideEnvironment(env)
        serviceFactory(newFn)
      })
    ZLayer.scoped[R](io)
  }
}

final class ZRunnable3[A1, A2, A3, R, E, V](fn: (A1, A2, A3) => ZIO[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: ((A1, A2, A3) => ZIO[Any, E, V]) => S): URLayer[R, S] = {
    val io = ZIO
      .environmentWith[R with Scope](env => {
        val newFn = (arg1: A1, arg2: A2, arg3: A3) => fn(arg1, arg2, arg3).provideEnvironment(env)
        serviceFactory(newFn)
      })
    ZLayer.scoped[R](io)
  }
}

final class ZRunnable4[A1, A2, A3, A4, R, E, V](fn: (A1, A2, A3, A4) => ZIO[R, E, V]) {
  def toLayer[S: zio.Tag](serviceFactory: ((A1, A2, A3, A4) => ZIO[Any, E, V]) => S): URLayer[R, S] = {
    val io = ZIO
      .environmentWith[R](env => {
        val newFn = (arg1: A1, arg2: A2, arg3: A3, arg4: A4) => fn(arg1, arg2, arg3, arg4).provideEnvironment(env)
        serviceFactory(newFn)
      })
    ZLayer.scoped[R](io)
  }
}

object ZRunnable {
  def apply[A, R, E, V](fn: A => ZIO[R, E, V]): ZRunnable1[A, R, E, V] = new ZRunnable1[A, R, E, V](fn)
  def apply[A1, A2, R, E, V](fn: (A1, A2) => ZIO[R, E, V]): ZRunnable2[A1, A2, R, E, V] =
    new ZRunnable2[A1, A2, R, E, V](fn)
  def apply[A1, A2, A3, R, E, V](fn: (A1, A2, A3) => ZIO[R, E, V]): ZRunnable3[A1, A2, A3, R, E, V] =
    new ZRunnable3[A1, A2, A3, R, E, V](fn)
  def apply[A1, A2, A3, A4, R, E, V](fn: (A1, A2, A3, A4) => ZIO[R, E, V]): ZRunnable4[A1, A2, A3, A4, R, E, V] =
    new ZRunnable4[A1, A2, A3, A4, R, E, V](fn)
}
