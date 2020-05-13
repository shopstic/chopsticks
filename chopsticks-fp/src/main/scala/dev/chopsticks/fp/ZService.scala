package dev.chopsticks.fp

import zio.{Has, Tagged, URIO, ZIO}

object ZService {
  def get[V](env: Has[V])(implicit tagged: Tagged[V]): V = {
    env.get[V]
  }

  def access[V](implicit tagged: Tagged[V]): URIO[Has[V], V] = {
    ZIO.access[Has[V]](_.get[V])
  }

  def apply[V](implicit tagged: Tagged[V]): URIO[Has[V], V] = {
    ZIO.access[Has[V]](_.get[V])
  }
}
