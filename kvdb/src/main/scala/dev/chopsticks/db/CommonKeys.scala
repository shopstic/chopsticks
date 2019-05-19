package dev.chopsticks.db

import dev.chopsticks.codec.DbKey

//noinspection TypeAnnotation
object CommonKeys {
  final case class IntIdKey(id: Int) extends AnyVal
  object IntIdKey {
    implicit val dbKey = DbKey[IntIdKey]
//    implicit val dbKeyPrefix = DbKeyPrefix[Int, IntIdKey]
  }
}
