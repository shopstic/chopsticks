package dev.chopsticks.util.log

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import akka.event.slf4j.Slf4jLogger

final class AkkaSlf4jLogger extends Slf4jLogger {
  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")
  private val defaultZoneId = ZoneId.systemDefault()

  override protected def formatTimestamp(timestamp: Long): String = {
    val d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), defaultZoneId)
    timestampFormatter.format(d)
  }
}
