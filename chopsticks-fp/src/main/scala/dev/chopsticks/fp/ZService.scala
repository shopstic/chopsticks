// todo remove this file
//package dev.chopsticks.fp
//import zio.Tag
//
//import zio.{URIO, ZIO}
//
//object ZService {
//  def get[V: Tag](env: V): V = {
//    env.get[V]
//  }
//
//  def access[V: Tag]: URIO[Has[V], V] = {
//    ZIO.access[Has[V]](_.get[V])
//  }
//
//  def apply[V: Tag]: URIO[Has[V], V] = {
//    ZIO.access[Has[V]](_.get[V])
//  }
//}
