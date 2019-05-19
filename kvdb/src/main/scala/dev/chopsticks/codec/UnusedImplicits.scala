package dev.chopsticks.codec

object UnusedImplicits {
  implicit final class UnusedOps[A](private val a: A) extends AnyVal {
    def unused(): Unit = ()
  }
}
