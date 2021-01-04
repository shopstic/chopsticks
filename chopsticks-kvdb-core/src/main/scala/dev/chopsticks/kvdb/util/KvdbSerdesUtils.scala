package dev.chopsticks.kvdb.util

import java.time.{Instant, LocalDateTime, ZoneId}
import java.nio.charset.StandardCharsets.UTF_8

object KvdbSerdesUtils {
  private val NANOS_IN_A_SECOND = 1000000000

  def epochNanosToInstant(value: BigInt): Instant = {
    val nanoSeconds = (value % NANOS_IN_A_SECOND).toLong
    val seconds = (value / NANOS_IN_A_SECOND).toLong
    Instant.ofEpochSecond(seconds, nanoSeconds)
  }

  def localDateTimeToEpochNanos(value: LocalDateTime, zoneId: ZoneId): BigInt = {
    val seconds = BigInt(value.atZone(zoneId).toEpochSecond)
    val nanoSeconds = value.getNano
    seconds * NANOS_IN_A_SECOND + nanoSeconds
  }

  def epochNanosToLocalDateTime(value: BigInt, zoneId: ZoneId): LocalDateTime = {
    epochNanosToInstant(value).atZone(zoneId).toLocalDateTime
  }

  def instantToEpochNanos(value: Instant): BigInt = {
    val seconds = BigInt(value.getEpochSecond)
    val nanoSeconds = value.getNano
    seconds * NANOS_IN_A_SECOND + nanoSeconds
  }

  def stringToByteArray(string: String): Array[Byte] = {
    string.getBytes(UTF_8)
  }

  def byteArrayToString(bytes: Array[Byte]): String = {
    new String(bytes, UTF_8)
  }
}
