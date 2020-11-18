package dev.chopsticks.kvdb.codec.compressed

final class SnappyCompressed[A](val value: A) extends AnyVal

trait CompressionDescription[A] {
  def minCompressionBlockSize: Int
}
