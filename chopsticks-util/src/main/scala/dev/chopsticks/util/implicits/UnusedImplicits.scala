package dev.chopsticks.util.implicits

object UnusedImplicits {
  implicit final class UnusedOps[A](private val a: A) extends AnyVal {
    def unused(): Unit = ()
  }
}
