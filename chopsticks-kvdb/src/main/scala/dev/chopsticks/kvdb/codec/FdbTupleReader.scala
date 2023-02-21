package dev.chopsticks.kvdb.codec

import java.math.BigInteger
import java.util.UUID

import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

/** Intentionally not thread-safe.
  * @param tuple
  *   Tuple
  */
final class FdbTupleReader(tuple: Tuple) {
  private var currentIndex = 0

  private def advanceIndex(): Int = {
    val index = currentIndex
    currentIndex = currentIndex + 1
    index
  }

  def getLong: Long = {
    tuple.getLong(advanceIndex())
  }

  def getBytes: Array[Byte] = {
    tuple.getBytes(advanceIndex())
  }

  def getString: String = {
    tuple.getString(advanceIndex())
  }

  def getBigInteger: BigInteger = {
    tuple.getBigInteger(advanceIndex())
  }

  def getFloat: Float = {
    tuple.getFloat(advanceIndex())
  }

  def getDouble: Double = {
    tuple.getDouble(advanceIndex())
  }

  def getBoolean: Boolean = {
    tuple.getBoolean(advanceIndex())
  }

  def getUUID: UUID = {
    tuple.getUUID(advanceIndex())
  }

  def getVersionStamp: Versionstamp = {
    tuple.getVersionstamp(advanceIndex())
  }

  override def toString: String = s"currentIndex=${currentIndex} tuple=${tuple.toString}"
}
