package dev.chopsticks.fp
import zio.Tag

import zio.{Has, URIO, ZIO}

object ZService {
  def get[V: Tag](env: Has[V]): V = {
    env.get[V]
  }

  def access[V: Tag]: URIO[Has[V], V] = {
    ZIO.access[Has[V]](_.get[V])
  }

  def apply[V: Tag]: URIO[Has[V], V] = {
    ZIO.access[Has[V]](_.get[V])
  }
}
