package dev.chopsticks.util.config

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, Duration => JavaDuration}

import akka.actor.ActorPath
import akka.util.{ByteString, Timeout}
import com.typesafe.config.ConfigValue
import eu.timepit.refined.api.{RefType, Validate}
import pureconfig.ConfigConvert.{viaNonEmptyString, viaNonEmptyStringTry, viaString}
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}
import pureconfig.generic.{ExportMacros, ProductHint}
import pureconfig.{ConfigConvert, ConfigCursor, ConfigReader, ConfigWriter, Exported}
import squants.information.Information
import scala.reflect.runtime.universe.WeakTypeTag
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.experimental.macros

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

  implicit def refTypeConfigConvert[F[_, _], T, P](implicit
    configConvert: ConfigConvert[T],
    refType: RefType[F],
    validate: Validate[T, P],
    typeTag: WeakTypeTag[F[T, P]]
  ): ConfigConvert[F[T, P]] = new ConfigConvert[F[T, P]] {
    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, F[T, P]] =
      configConvert.from(cur) match {
        case Right(t) =>
          refType.refine[P](t) match {
            case Left(because) =>
              Left(
                ConfigReaderFailures(
                  ConvertFailure(
                    reason = CannotConvert(
                      value = cur.value.render(),
                      toType = typeTag.tpe.toString,
                      because = because
                    ),
                    cur = cur
                  )
                )
              )

            case Right(refined) =>
              Right(refined)
          }

        case Left(configReaderFailures) =>
          Left(configReaderFailures)
      }

    override def to(t: F[T, P]): ConfigValue =
      configConvert.to(refType.unwrap(t))
  }

  implicit def exportReader[A]: Exported[ConfigReader[A]] = macro ExportMacros.exportDerivedReader[A]
  implicit def exportWriter[A]: Exported[ConfigWriter[A]] = macro ExportMacros.exportDerivedWriter[A]
}
