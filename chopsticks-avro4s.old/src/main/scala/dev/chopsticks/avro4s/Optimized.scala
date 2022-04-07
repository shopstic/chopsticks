package dev.chopsticks.avro4s

trait Optimized[T]
object Optimized {
  private object singleton extends Optimized[Any]
  def apply[T]: Optimized[T] = singleton.asInstanceOf[Optimized[T]]
}
