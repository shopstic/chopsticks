package dev.chopsticks.util.config

import java.time.{LocalDate, LocalDateTime, LocalTime, Duration => JavaDuration}
import java.time.format.DateTimeFormatter

import akka.actor.ActorPath
import akka.util.{ByteString, Timeout}
import pureconfig.ConfigConvert
import pureconfig.ConfigConvert.{viaNonEmptyString, viaNonEmptyStringTry, viaString}
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.generic.ProductHint
import squants.information.Information

import scala.concurrent.duration.{Duration, FiniteDuration}

object PureconfigConverters {
  import pureconfig.configurable.{localDateConfigConvert, localTimeConfigConvert}

  implicit val informationConfigConverter: ConfigConvert[Information] =
    viaNonEmptyStringTry[Information](Information.apply, _.toString)
  implicit val localTimeConfigConverter: ConfigConvert[LocalTime] = localTimeConfigConvert(DateTimeFormatter.ISO_TIME)
  implicit val localDateConfigConverter: ConfigConvert[LocalDate] = localDateConfigConvert(DateTimeFormatter.ISO_DATE)
  implicit val localDateTimeConfigConverter: ConfigConvert[LocalDateTime] = viaNonEmptyString[LocalDateTime](
    catchReadError(LocalDateTime.parse(_, DateTimeFormatter.ISO_DATE_TIME)),
    _.toString
  )
  implicit val byteStringConfigConverter: ConfigConvert[ByteString] = viaNonEmptyString[ByteString](
    catchReadError(ByteString.apply),
    _.utf8String
  )
  implicit val bigIntConfigConverter: ConfigConvert[BigInt] = viaNonEmptyString[BigInt](
    catchReadError(BigInt.apply),
    _.toString
  )

  implicit val javaDurationConfigConverter: ConfigConvert[JavaDuration] = viaNonEmptyString[JavaDuration](
    catchReadError(v => JavaDuration.ofNanos(Duration(v).toNanos)),
    v => Duration.fromNanos(v.getNano.toLong).toString
  )

  implicit def hint[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  //    implicit val informationConfigConvert: ConfigConvert[Information] = viaNonEmptyStringTry[Information](v => Information(v), _.toString)

  implicit val timeoutCC: ConfigConvert[Timeout] = ConfigConvert[FiniteDuration].xmap(new Timeout(_), _.duration)

  implicit val actorPathCC: ConfigConvert[ActorPath] =
    viaString[ActorPath](catchReadError(ActorPath.fromString), _.toSerializationFormat)

  implicit val intMapConfigConverter: ConfigConvert[Map[Int, Int]] = {
    ConfigConvert[Map[String, Int]].xmap(_.map(p => (p._1.toInt, p._2)), _.map(p => (p._1.toString, p._2)))
  }
}
