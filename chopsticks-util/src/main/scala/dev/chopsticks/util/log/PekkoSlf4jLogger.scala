package dev.chopsticks.util.log

import org.apache.pekko.event.slf4j.Slf4jLogger
import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

final class PekkoSlf4jLogger extends Slf4jLogger {

  // We don't want the initial "Slf4jLogger started" info log
  override lazy val log: Logger = NOPLogger.NOP_LOGGER

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")
  private val defaultZoneId = ZoneId.systemDefault()

  override protected def formatTimestamp(timestamp: Long): String = {
    val d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), defaultZoneId)
    timestampFormatter.format(d)
  }
}
