package dev.chopsticks.fp

import zio.{Has, Tagged, URIO, ZIO}

object ZService {
  def apply[V](implicit tagged: Tagged[V]): URIO[Has[V], V] = {
    ZIO.access[Has[V]](_.get[V])
  }
}
