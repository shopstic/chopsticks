package dev.chopsticks.schema.config

import pureconfig.ConfigConvert
import zio.schema.Schema

trait SchemaConfig[A]:
  implicit def zioSchema: Schema[A]
  implicit lazy val configConvert: ConfigConvert[A] =
    ConfigConvert.fromReaderAndWriter(
      SchemaConfigReaderConverter.convert(zioSchema),
      SchemaConfigWriterConverter.convert(zioSchema)
    )
