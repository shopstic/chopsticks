package dev.chopsticks.schema.config

import com.typesafe.config.*
import pureconfig.{
  ConfigConvert,
  ConfigCursor,
  ConfigListCursor,
  ConfigObjectCursor,
  ConfigReader,
  ConfigWriter,
  SimpleConfigCursor
}
import pureconfig.ConfigReader.Result
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure, FailureReason}
import zio.Chunk

object SchemaConfigConverters:
  implicit val unitConverter: ConfigConvert[Unit] =
    new ConfigConvert[Unit]:
      override def from(cur: ConfigCursor): Result[Unit] =
        cur match
          case c: ConfigListCursor =>
            if (c.isEmpty) Right(())
            else Left(convertError(
              CannotConvert(c.toString, "Unit", "ConfigListCursor can be converted to Unit only if it's empty."),
              c
            ))
          case c: ConfigObjectCursor =>
            if (c.isEmpty) Right(())
            else Left(convertError(
              CannotConvert(c.toString, "Unit", "ConfigObjectCursor can be converted to Unit only if it's empty."),
              c
            ))
          case c: SimpleConfigCursor =>
            if (c.isNull) Right(())
            else Left(convertError(
              CannotConvert(c.toString, "Unit", "SimpleConfigCursor can be converted to Unit only if it's null."),
              c
            ))
      override def to(a: Unit): ConfigValue =
        ConfigValueFactory.fromMap(java.util.Map.of())

  def chunkReader[A: ConfigReader]: ConfigReader[Chunk[A]] =
    ConfigReader.traversableReader[A, Chunk]

  def chunkWriter[A: ConfigWriter]: ConfigWriter[Chunk[A]] =
    ConfigWriter.traversableWriter[A, Chunk]

  def setReader[A: ConfigReader]: ConfigReader[Set[A]] =
    ConfigReader.traversableReader[A, Set]

  def setWriter[A: ConfigWriter]: ConfigWriter[Set[A]] =
    ConfigWriter.traversableWriter[A, Set]

  private def convertError(failure: FailureReason, cur: ConfigCursor) =
    ConfigReaderFailures(ConvertFailure(failure, cur))
