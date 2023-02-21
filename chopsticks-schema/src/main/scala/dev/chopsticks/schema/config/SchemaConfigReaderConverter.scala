package dev.chopsticks.schema.config

import dev.chopsticks.schema.{SchemaAnnotation, SchemaAnnotations, SchemaSumTypeSerDeStrategy, SchemaValidation}
import pureconfig.{ConfigCursor, ConfigReader, ReadsMissingKeys}
import pureconfig.ConfigReader.Result
import pureconfig.error.{ConfigReaderFailure, ConfigReaderFailures, ConvertFailure, UserValidationFailed}
import zio.schema.{FieldSet, Schema, StandardType, TypeId}
import zio.Chunk

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.ListMap

object SchemaConfigReaderConverter:
  private val cache = TrieMap.empty[CacheKey, LazyReader[_]]

  final private case class CacheKey(typeId: Option[TypeId], entityName: String, annotationsHash: Int)

  def convert[A](schema: Schema[A]): ConfigReader[A] =
    new Converter(cache).convert(schema)

  final private[SchemaConfigReaderConverter] class LazyReader[A]() extends ConfigReader[A]:
    private var _reader: ConfigReader[A] = _
    private[SchemaConfigReaderConverter] def set(reader: ConfigReader[A]): Unit =
      this._reader = reader
    private def get: ConfigReader[A] =
      if (_reader == null) throw new RuntimeException("LazyReader has not yet been initialized")
      else _reader
    override def from(cur: ConfigCursor): ConfigReader.Result[A] = get.from(cur)
  end LazyReader

  private class Converter(cache: TrieMap[CacheKey, LazyReader[_]]):
    private def convertUsingCache[A](annotations: SchemaAnnotations[A])(convert: => ConfigReader[A]): ConfigReader[A] =
      annotations.entityName match
        case Some(entityName) =>
          val cacheKey = CacheKey(annotations.typeId, entityName, annotations.hashCode())
          cache.get(cacheKey) match
            case Some(value) => value.asInstanceOf[ConfigReader[A]]
            case None =>
              val lazyReader = new LazyReader[A]()
              val _ = cache.addOne(cacheKey -> lazyReader)
              val result = convert
              lazyReader.set(result)
              result
//                lazyReader
        case None => convert

    def convert[A](schema: Schema[A]): ConfigReader[A] =
      //scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
      schema match
        case Schema.Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, fromChunk, _, annotations, _) =>
          addAnnotations(
            SchemaConfigConverters.chunkReader(convert(schemaA)).map(fromChunk),
            SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case Schema.Map(_, _, _) => ???

        case Schema.Set(schemaA, annotations) =>
          addAnnotations(
            SchemaConfigConverters.setReader(convert(schemaA)),
            SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case Schema.Transform(schemaA, f, _, annotations, _) =>
          val baseReader = convert(schemaA).emap { elem =>
            f(elem).left.map(error => UserValidationFailed(error))
          }
          addAnnotations(baseReader, SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations))

        case _: Schema.Tuple2[_, _] =>
          ???

        case Schema.Optional(schemaA, annotations) =>
          addAnnotations[A](
            baseReader = ConfigReader.optionReader(convert(schemaA)),
            metadata = SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema)

        case _: Schema.Fail[_] =>
          ???

        case either @ Schema.Either(_, _, _) =>
          convert(either.toEnum)

        case Schema.CaseClass0(typeId, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            addAnnotations(
              SchemaConfigConverters.unitConverter.map(_ => construct()),
              parsed
            )
          }

//          case Schema.CaseClass1(typeId, f1, construct, annotations) =>
//            val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
//            convertUsingCache(parsed) {
//              val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations))
//              val baseReader = ConfigReader.forProduct1(
//                parsed.transformConfigLabel(f1.name)
//              )(construct)(reader1)
//              addAnnotations(
//                baseReader,
//                parsed
//              )
//            }

        case s: Schema.CaseClass1[_, _] =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(s.id), s.annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(s.field.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = s.field.annotations))
            val baseReader = ConfigReader.forProduct1(
              parsed.transformConfigLabel(s.field.name)
            )(s.defaultConstruct)(reader1)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass2(typeId, f1, f2, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val baseReader = ConfigReader.forProduct2(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name)
            )(construct)(reader1, reader2)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass3(typeId, f1, f2, f3, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val baseReader = ConfigReader.forProduct3(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name)
            )(construct)(reader1, reader2, reader3)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass4(typeId, f1, f2, f3, f4, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val baseReader = ConfigReader.forProduct4(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name)
            )(construct)(reader1, reader2, reader3, reader4)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass5(typeId, f1, f2, f3, f4, f5, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val baseReader = ConfigReader.forProduct5(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass6(typeId, f1, f2, f3, f4, f5, f6, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val baseReader = ConfigReader.forProduct6(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass7(typeId, f1, f2, f3, f4, f5, f6, f7, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val baseReader = ConfigReader.forProduct7(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass8(typeId, f1, f2, f3, f4, f5, f6, f7, f8, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val baseReader = ConfigReader.forProduct8(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass9(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val baseReader = ConfigReader.forProduct9(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass10(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val baseReader = ConfigReader.forProduct10(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass11(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val baseReader = ConfigReader.forProduct11(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass12(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val baseReader = ConfigReader.forProduct12(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass13(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val baseReader = ConfigReader.forProduct13(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass14(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val baseReader = ConfigReader.forProduct14(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass15(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val baseReader = ConfigReader.forProduct15(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass16(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val baseReader = ConfigReader.forProduct16(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass17(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val baseReader = ConfigReader.forProduct17(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass18(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val reader18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val baseReader = ConfigReader.forProduct18(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name),
              parsed.transformConfigLabel(f18.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17, reader18)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass19(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, construct, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val reader18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val reader19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val baseReader = ConfigReader.forProduct19(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name),
              parsed.transformConfigLabel(f18.name),
              parsed.transformConfigLabel(f19.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17, reader18, reader19)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass20(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (construct, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val reader18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val reader19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val reader20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val baseReader = ConfigReader.forProduct20(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name),
              parsed.transformConfigLabel(f18.name),
              parsed.transformConfigLabel(f19.name),
              parsed.transformConfigLabel(f20.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17, reader18, reader19, reader20)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass21(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (f21, construct, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val reader18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val reader19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val reader20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val reader21 = addAnnotations(convert(f21.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f21.annotations))
            val baseReader = ConfigReader.forProduct21(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name),
              parsed.transformConfigLabel(f18.name),
              parsed.transformConfigLabel(f19.name),
              parsed.transformConfigLabel(f20.name),
              parsed.transformConfigLabel(f21.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17, reader18, reader19, reader20, reader21)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.CaseClass22(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (f21, f22, construct, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val reader1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val reader2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val reader3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val reader4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val reader5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val reader6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val reader7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val reader8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val reader9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val reader10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val reader11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val reader12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val reader13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val reader14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val reader15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val reader16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val reader17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val reader18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val reader19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val reader20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val reader21 = addAnnotations(convert(f21.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f21.annotations))
            val reader22 = addAnnotations(convert(f22.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f22.annotations))
            val baseReader = ConfigReader.forProduct22(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name),
              parsed.transformConfigLabel(f10.name),
              parsed.transformConfigLabel(f11.name),
              parsed.transformConfigLabel(f12.name),
              parsed.transformConfigLabel(f13.name),
              parsed.transformConfigLabel(f14.name),
              parsed.transformConfigLabel(f15.name),
              parsed.transformConfigLabel(f16.name),
              parsed.transformConfigLabel(f17.name),
              parsed.transformConfigLabel(f18.name),
              parsed.transformConfigLabel(f19.name),
              parsed.transformConfigLabel(f20.name),
              parsed.transformConfigLabel(f21.name),
              parsed.transformConfigLabel(f22.name)
            )(construct)(reader1, reader2, reader3, reader4, reader5, reader6, reader7, reader8, reader9, reader10, reader11, reader12, reader13, reader14, reader15, reader16, reader17, reader18, reader19, reader20, reader21, reader22)
            addAnnotations(
              baseReader,
              parsed
            )
          }

        case Schema.GenericRecord(typeId, fieldSet, annotations) =>
          genericRecordConverter(typeId, fieldSet, annotations)

        case Schema.Enum1(typeId, c1, annotations) =>
          convertEnum[A](typeId, annotations, c1)

        case Schema.Enum2(typeId, c1, c2, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2)

        case Schema.Enum3(typeId, c1, c2, c3, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3)

        case Schema.Enum4(typeId, c1, c2, c3, c4, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4)

        case Schema.Enum5(typeId, c1, c2, c3, c4, c5, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5)

        case Schema.Enum6(typeId, c1, c2, c3, c4, c5, c6, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6)

        case Schema.Enum7(typeId, c1, c2, c3, c4, c5, c6, c7, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7)

        case Schema.Enum8(typeId, c1, c2, c3, c4, c5, c6, c7, c8, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8)

        case Schema.Enum9(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)

        case Schema.Enum10(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)

        case Schema.Enum11(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)

        case Schema.Enum12(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)

        case Schema.Enum13(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)

        case Schema.Enum14(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)

        case Schema.Enum15(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)

        case Schema.Enum16(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)

        case Schema.Enum17(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)

        case Schema.Enum18(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)

        case Schema.Enum19(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)

        case Schema.Enum20(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)

        case Schema.Enum21(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)

        case Schema.Enum22(typeId, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
          convertEnum[A](typeId, annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)

        case Schema.EnumN(typeId, cases, annotations) =>
          convertEnum[A](typeId, annotations, cases.toSeq: _*)

        case _: Schema.Dynamic =>
          ???
    //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }
    end convert

    private def extractTypeId(schema: Schema[_]): Option[TypeId] =
      schema match
        case r: Schema.Record[_] => Some(r.id)
        case _: Schema.Collection[_, _] => None
        case _: Schema.Enum[_] => None
        case _: Schema.Transform[_, _, _] => None
        case _: Schema.Primitive[_] => None
        case _: Schema.Optional[_] => None
        case _: Schema.Fail[_] => None
        case _: Schema.Tuple2[_, _] => None
        case _: Schema.Either[_, _] => None
        case l: Schema.Lazy[_] => extractTypeId(l.schema)
        case _: Schema.Dynamic => None

    private def convertEnum[A](typeId: TypeId, annotations: Chunk[Any], cases: Schema.Case[A, _]*): ConfigReader[A] =
      val enumAnnotations = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
      val readersByName = cases
        .iterator
        .map { c =>
          // todo extract typeId from the childSchema
          // todo this should be a recursive fn
          val cAnn = SchemaAnnotations.extractAnnotations[Any](
            extractTypeId(c.schema),
            c.annotations
          )
          val reader = addAnnotations(convert(c.schema.asInstanceOf[Schema[Any]]), cAnn)
          val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive an ConfigReader. Received annotations: $cAnn"
          ))
          entityName -> reader
        }
        .toMap
      val serDeStrategy: SchemaAnnotation.SumTypeSerDeStrategy[A] =
        enumAnnotations.sumTypeSerDeStrategy.getOrElse(throw new RuntimeException(
          s"Discriminator must be defined to derive an ConfigReader. Received annotations: $enumAnnotations"
        ))
      val reader =
        serDeStrategy.value match
          case SchemaSumTypeSerDeStrategy.Discriminator(discriminator) =>
            val diff = discriminator.mapping.values.toSet.diff(readersByName.keySet)
            if (diff.nonEmpty) {
              throw new RuntimeException(
                s"Cannot derive io.circe.Decoder for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and decoders don't match. Diff=$diff."
              )
            }
            new ConfigReader[A]:
              private val knownObjectTypes = discriminator.mapping.keys.toList.sorted.mkString(", ")
              override def from(cur: ConfigCursor): Result[A] =
                for
                  objectCursor <- cur.asObjectCursor
                  discriminatorCursor <- objectCursor.atKey(discriminator.discriminatorFieldName)
                  discriminatorValue <- ConfigReader.stringConfigReader.from(discriminatorCursor)
                  result <-
                    discriminator.mapping.get(discriminatorValue) match
                      case Some(objType) => readersByName(objType).from(cur).asInstanceOf[Result[A]]
                      case None =>
                        Left(ConfigReaderFailures(ConvertFailure(
                          UserValidationFailed(
                            s"Unrecognized object type: $discriminatorValue. Valid object types are: $knownObjectTypes."
                          ),
                          cur
                        )))
                yield result
      addAnnotations(reader, enumAnnotations)
    end convertEnum

    private def genericRecordConverter(
      typeId: TypeId,
      fieldSet: FieldSet,
      annotations: Chunk[Any]
    ): ConfigReader[ListMap[String, _]] =
      val parsed = SchemaAnnotations.extractAnnotations[ListMap[String, _]](Some(typeId), annotations)
      convertUsingCache(parsed) {
        val fieldReaders = fieldSet.toChunk.map { field =>
          val fieldReader =
            addAnnotations(convert(field.schema), SchemaAnnotations.extractAnnotations(None, field.annotations))
          parsed.transformJsonLabel(field.name) -> (field.name, fieldReader)
        }
        val baseReader = new ConfigReader[ListMap[String, _]] {
          override def from(cur: ConfigCursor): Result[ListMap[String, _]] =
            cur.asObjectCursor.flatMap { objectCursor =>
              val errorsBuilder = Chunk.newBuilder[ConfigReaderFailure]
              val builder = ListMap.newBuilder[String, Any]
              for ((mappedKey, (key, reader)) <- fieldReaders) do
                reader.from(objectCursor.atKeyOrUndefined(mappedKey)) match
                  case Left(error) => val _ = errorsBuilder.addOne(error.head).addAll(error.tail)
                  case Right(value) => val _ = builder.addOne(key -> value)
              val errors = errorsBuilder.result()
              if (errors.isEmpty) Right(builder.result())
              else Left(ConfigReaderFailures(errors.head, errors.tail: _*))
            }
        }
        addAnnotations(baseReader, parsed)
      }

    end genericRecordConverter

    private def primitiveConverter[A](standardType: StandardType[A], annotations: Chunk[Any]): ConfigReader[A] =
      val baseReader =
        standardType match
          case StandardType.UnitType => SchemaConfigConverters.unitConverter
          case StandardType.ByteType => ???
          case StandardType.StringType => ConfigReader.stringConfigReader
          case StandardType.BoolType => ConfigReader.booleanConfigReader
          case StandardType.ShortType => ConfigReader.shortConfigReader
          case StandardType.IntType => ConfigReader.intConfigReader
          case StandardType.LongType => ConfigReader.longConfigReader
          case StandardType.FloatType => ConfigReader.floatConfigReader
          case StandardType.DoubleType => ConfigReader.doubleConfigReader
          case StandardType.BinaryType => ???
          case StandardType.CharType => ConfigReader.charConfigReader
          case StandardType.BigIntegerType => ConfigReader.javaBigIntegerReader
          case StandardType.BigDecimalType => ConfigReader.javaBigDecimalReader
          case StandardType.UUIDType => ConfigReader.uuidConfigReader
          case StandardType.DayOfWeekType => ???
          case StandardType.DurationType => ConfigReader.javaDurationConfigReader
          case StandardType.InstantType => ConfigReader.instantConfigReader
          case StandardType.LocalDateType => ???
          case StandardType.LocalDateTimeType => ???
          case StandardType.LocalTimeType => ???
          // todo move it to object and assign as implicit val
          case StandardType.MonthType => ???
          case StandardType.MonthDayType => ???
          case StandardType.OffsetDateTimeType => ???
          case StandardType.OffsetTimeType => ???
          case StandardType.PeriodType => ConfigReader.periodConfigReader
          case StandardType.YearType => ConfigReader.yearConfigReader
          case StandardType.YearMonthType => ??? // ConfigReader.yearMo
          case StandardType.ZonedDateTimeType => ??? ///io.circe.Encoder[ZonedDateTime]
          case StandardType.ZoneIdType => ConfigReader.zoneIdConfigReader
          case StandardType.ZoneOffsetType => ConfigReader.zoneOffsetConfigReader

      addAnnotations(
        baseReader.asInstanceOf[ConfigReader[A]],
        SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
      )

    end primitiveConverter

    private def addAnnotations[A](
      baseReader: ConfigReader[A],
      metadata: SchemaAnnotations[A]
    ): ConfigReader[A] = {
      val readerWithDefault = metadata.default.fold(baseReader) { default =>
        new ConfigReader[A] with ReadsMissingKeys:
          override def from(cur: ConfigCursor): ConfigReader.Result[A] = {
            if (cur.isUndefined || cur.isNull) Right(default.value)
            else baseReader.from(cur)
          }
      }
      val readerWithValidation = metadata.validate.fold(readerWithDefault) { validate =>
        new ConfigReader[A] with ReadsMissingKeys:
          override def from(cur: ConfigCursor): Result[A] =
            readerWithDefault.from(cur) match
              case errors @ Left(_) => errors
              case right @ Right(value) =>
                val errors: List[ConfigReaderFailure] = validate.validator.apply(value).map { validationError =>
                  ConvertFailure(
                    UserValidationFailed(SchemaValidation.errorMessage(validationError)),
                    cur
                  )
                }
                if (errors.isEmpty) right
                else Left(ConfigReaderFailures(errors.head, errors.tail: _*))
      }
      readerWithValidation
    }

  end Converter

end SchemaConfigReaderConverter
