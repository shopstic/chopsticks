package dev.chopsticks.schema.config

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}
import dev.chopsticks.schema.{SchemaAnnotations, SchemaSumTypeSerDeStrategy}
import pureconfig.ConfigWriter

import scala.collection.concurrent.TrieMap
import zio.schema.*
import zio.Chunk

import scala.annotation.nowarn
import scala.collection.immutable.ListMap

object SchemaConfigWriterConverter:

  private val cache = TrieMap.empty[CacheKey, LazyWriter[_]]

  final private case class CacheKey(typeId: Option[TypeId], entityName: String, annotationsHash: Int)

  final private[SchemaConfigWriterConverter] class LazyWriter[A]() extends ConfigWriter[A]:
    private var _writer: ConfigWriter[A] = _

    private[SchemaConfigWriterConverter] def set(writer: ConfigWriter[A]): Unit =
      this._writer = writer

    private def get: ConfigWriter[A] =
      if (_writer == null) throw new RuntimeException("LazyReader has not yet been initialized")
      else _writer

    override def to(a: A): ConfigValue = get.to(a)
  end LazyWriter

  def convert[A](schema: Schema[A]): ConfigWriter[A] =
    new Converter(cache).convert(schema)

  private class Converter(cache: TrieMap[CacheKey, LazyWriter[_]]):
    private def convertUsingCache[A](annotations: SchemaAnnotations[A])(convert: => ConfigWriter[A]): ConfigWriter[A] =
      annotations.entityName match
        case Some(entityName) =>
          val cacheKey = CacheKey(annotations.typeId, entityName, annotations.hashCode())
          cache.get(cacheKey) match
            case Some(value) => value.asInstanceOf[ConfigWriter[A]]
            case None =>
              val lazyWriter = new LazyWriter[A]()
              val _ = cache.addOne(cacheKey -> lazyWriter)
              val result = convert
              lazyWriter.set(result)
              result
        case None => convert

    def convert[A](schema: Schema[A]): ConfigWriter[A] =
      //scalafmt: { maxColumn = 800, optIn.configStyleArguments = false }
      schema match
        case Schema.Primitive(standardType, annotations) =>
          primitiveConverter(standardType, annotations)

        case Schema.Sequence(schemaA, _, toChunk, annotations, _) =>
          addAnnotations(
            SchemaConfigConverters.chunkWriter(convert(schemaA)).contramap(toChunk),
            SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case Schema.Map(_, _, _) => ???

        case Schema.Set(schemaA, annotations) =>
          addAnnotations(
            SchemaConfigConverters.setWriter(convert(schemaA)),
            SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case Schema.Transform(schemaA, _, g, annotations, _) =>
          val baseWriter = convert(schemaA).contramap[A] { x =>
            g(x) match
              case Right(v) => v
              case Left(error) => throw new RuntimeException(s"Couldn't transform schema: $error")
          }
          addAnnotations(baseWriter, SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations))

        case _: Schema.Tuple2[_, _] => ???

        case Schema.Optional(schemaA, annotations) =>
          addAnnotations[A](
            ConfigWriter.optionWriter(convert(schemaA)),
            metadata = SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
          )

        case l @ Schema.Lazy(_) =>
          convert(l.schema)

        case _: Schema.Fail[_] => ???

        case either @ Schema.Either(_, _, _) =>
          convert(either.toEnum)

        case Schema.CaseClass0(typeId, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            addAnnotations(
              SchemaConfigConverters.unitConverter.contramap(_ => ()),
              parsed
            )
          }

        case s: Schema.CaseClass1[_, _] =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(s.id), s.annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(s.field.schema), SchemaAnnotations.extractAnnotations(None, s.field.annotations))
            val baseWriter = ConfigWriter.forProduct1(parsed.transformConfigLabel(s.field.name))(s.field.get)(writer1)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass2(typeId, f1, f2, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val baseWriter = ConfigWriter.forProduct2(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name)
            )((x: A) => (f1.get(x), f2.get(x)))(writer1, writer2)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass3(typeId, f1, f2, f3, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val baseWriter = ConfigWriter.forProduct3(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x)))(writer1, writer2, writer3)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass4(typeId, f1, f2, f3, f4, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val baseWriter = ConfigWriter.forProduct4(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x)))(writer1, writer2, writer3, writer4)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass5(typeId, f1, f2, f3, f4, f5, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val baseWriter = ConfigWriter.forProduct5(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x)))(writer1, writer2, writer3, writer4, writer5)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass6(typeId, f1, f2, f3, f4, f5, f6, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val baseWriter = ConfigWriter.forProduct6(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass7(typeId, f1, f2, f3, f4, f5, f6, f7, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val baseWriter = ConfigWriter.forProduct7(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass8(typeId, f1, f2, f3, f4, f5, f6, f7, f8, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val baseWriter = ConfigWriter.forProduct8(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass9(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val baseWriter = ConfigWriter.forProduct9(
              parsed.transformConfigLabel(f1.name),
              parsed.transformConfigLabel(f2.name),
              parsed.transformConfigLabel(f3.name),
              parsed.transformConfigLabel(f4.name),
              parsed.transformConfigLabel(f5.name),
              parsed.transformConfigLabel(f6.name),
              parsed.transformConfigLabel(f7.name),
              parsed.transformConfigLabel(f8.name),
              parsed.transformConfigLabel(f9.name)
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass10(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val baseWriter = ConfigWriter.forProduct10(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass11(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val baseWriter = ConfigWriter.forProduct11(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass12(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val baseWriter = ConfigWriter.forProduct12(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass13(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val baseWriter = ConfigWriter.forProduct13(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass14(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val baseWriter = ConfigWriter.forProduct14(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass15(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val baseWriter = ConfigWriter.forProduct15(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass16(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val baseWriter = ConfigWriter.forProduct16(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass17(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val baseWriter = ConfigWriter.forProduct17(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass18(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val writer18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val baseWriter = ConfigWriter.forProduct18(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x), f18.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17, writer18)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass19(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, annotations) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val writer18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val writer19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val baseWriter = ConfigWriter.forProduct19(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x), f18.get(x), f19.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17, writer18, writer19)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass20(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (_, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val writer18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val writer19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val writer20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val baseWriter = ConfigWriter.forProduct20(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x), f18.get(x), f19.get(x), f20.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17, writer18, writer19, writer20)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass21(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (f21, _, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val writer18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val writer19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val writer20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val writer21 = addAnnotations(convert(f21.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f21.annotations))
            val baseWriter = ConfigWriter.forProduct21(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x), f18.get(x), f19.get(x), f20.get(x), f21.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17, writer18, writer19, writer20, writer21)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.CaseClass22(typeId, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, (f21, f22, _, annotations)) =>
          val parsed = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
          convertUsingCache(parsed) {
            val writer1 = addAnnotations(convert(f1.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f1.annotations))
            val writer2 = addAnnotations(convert(f2.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f2.annotations))
            val writer3 = addAnnotations(convert(f3.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f3.annotations))
            val writer4 = addAnnotations(convert(f4.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f4.annotations))
            val writer5 = addAnnotations(convert(f5.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f5.annotations))
            val writer6 = addAnnotations(convert(f6.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f6.annotations))
            val writer7 = addAnnotations(convert(f7.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f7.annotations))
            val writer8 = addAnnotations(convert(f8.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f8.annotations))
            val writer9 = addAnnotations(convert(f9.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f9.annotations))
            val writer10 = addAnnotations(convert(f10.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f10.annotations))
            val writer11 = addAnnotations(convert(f11.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f11.annotations))
            val writer12 = addAnnotations(convert(f12.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f12.annotations))
            val writer13 = addAnnotations(convert(f13.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f13.annotations))
            val writer14 = addAnnotations(convert(f14.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f14.annotations))
            val writer15 = addAnnotations(convert(f15.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f15.annotations))
            val writer16 = addAnnotations(convert(f16.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f16.annotations))
            val writer17 = addAnnotations(convert(f17.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f17.annotations))
            val writer18 = addAnnotations(convert(f18.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f18.annotations))
            val writer19 = addAnnotations(convert(f19.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f19.annotations))
            val writer20 = addAnnotations(convert(f20.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f20.annotations))
            val writer21 = addAnnotations(convert(f21.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f21.annotations))
            val writer22 = addAnnotations(convert(f22.schema), SchemaAnnotations.extractAnnotations(typeId = None, annotations = f22.annotations))
            val baseWriter = ConfigWriter.forProduct22(
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
            )((x: A) => (f1.get(x), f2.get(x), f3.get(x), f4.get(x), f5.get(x), f6.get(x), f7.get(x), f8.get(x), f9.get(x), f10.get(x), f11.get(x), f12.get(x), f13.get(x), f14.get(x), f15.get(x), f16.get(x), f17.get(x), f18.get(x), f19.get(x), f20.get(x), f21.get(x), f22.get(x)))(writer1, writer2, writer3, writer4, writer5, writer6, writer7, writer8, writer9, writer10, writer11, writer12, writer13, writer14, writer15, writer16, writer17, writer18, writer19, writer20, writer21, writer22)
            addAnnotations(baseWriter, parsed)
          }

        case Schema.GenericRecord(typeId, fieldSet, annotations) =>
          genericRecordConverter(typeId, fieldSet, annotations)
//
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

        case Schema.EnumN(typeId, caseSet, annotations) =>
          convertEnum[A](typeId, annotations, caseSet.toSeq: _*)

        case _: Schema.Dynamic => ???
    //scalafmt: { maxColumn = 120, optIn.configStyleArguments = false }
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

    private def convertEnum[A](typeId: TypeId, annotations: Chunk[Any], cases: Schema.Case[A, _]*): ConfigWriter[A] =
      val enumAnnotations = SchemaAnnotations.extractAnnotations[A](Some(typeId), annotations)
      val writersByName = cases
        .iterator
        .map { c =>
          val cAnn = SchemaAnnotations.extractAnnotations(extractTypeId(c.schema), c.annotations)
          val writer = addAnnotations(convert(c.schema), SchemaAnnotations.extractAnnotations(None, c.annotations))
          val entityName = cAnn.entityName.getOrElse(throw new RuntimeException(
            s"Subtype of ${enumAnnotations.entityName.getOrElse("-")} must have entityName defined to derive an ConfigWriter. Received annotations: $cAnn"
          ))
          entityName -> (writer, c)
        }
        .toMap
      val serdeStrategy = enumAnnotations.sumTypeSerDeStrategy.getOrElse {
        throw new RuntimeException(
          s"Discriminator must be defined to derive an io.circe.Encoder. Received annotations: $enumAnnotations"
        )
      }
      val writer =
        serdeStrategy.value match
          case SchemaSumTypeSerDeStrategy.Discriminator(discriminator) =>
            val diff = discriminator.mapping.values.toSet.diff(writersByName.keySet)
            if (diff.nonEmpty) {
              throw new RuntimeException(
                s"Cannot derive ConfigWriter for ${enumAnnotations.entityName.getOrElse("-")}, because mapping and encoders don't match. Diff=$diff."
              )
            }
            new ConfigWriter[A]:
              override def to(a: A): ConfigValue =
                val discValue = discriminator.discriminatorValue(a)
                val (w, c) = writersByName(discriminator.mapping(discValue))
                w.asInstanceOf[ConfigWriter[Any]].to(c.deconstruct(a).asInstanceOf[Any]) match
                  case configObject: ConfigObject =>
                    configObject
                      .withValue(
                        discriminator.discriminatorFieldName,
                        ConfigWriter.stringConfigWriter.to(discValue)
                      )
                  case other: ConfigValue =>
                    val conf = ConfigFactory.empty()
                    conf
                      .withValue(
                        discriminator.discriminatorFieldName,
                        ConfigWriter.stringConfigWriter.to(discValue)
                      )
                      .withValue("value", other)
                      .root()
      addAnnotations(writer, enumAnnotations)
    end convertEnum

    private def genericRecordConverter(
      typeId: TypeId,
      fieldSet: FieldSet,
      annotations: Chunk[Any]
    ): ConfigWriter[ListMap[String, _]] =
      val parsed = SchemaAnnotations.extractAnnotations[ListMap[String, _]](Some(typeId), annotations)
      convertUsingCache(parsed) {
        val fieldWriters = fieldSet.toChunk.map { field =>
          addAnnotations(convert(field.schema), SchemaAnnotations.extractAnnotations(None, field.annotations))
        }
        val baseWriter =
          new ConfigWriter[ListMap[String, _]]:
            override def to(a: ListMap[String, _]): ConfigValue =
              a
                .iterator
                .zip(fieldWriters.iterator)
                .foldLeft(ConfigFactory.empty()) { case (acc, ((k, v), writer)) =>
                  acc.withValue(parsed.transformJsonLabel(k), writer.asInstanceOf[ConfigWriter[Any]].to(v))
                }
                .root()

        addAnnotations(baseWriter, parsed)
      }
    end genericRecordConverter

    private def primitiveConverter[A](standardType: StandardType[A], annotations: Chunk[Any]): ConfigWriter[A] =
      val baseWriter =
        standardType match
          case StandardType.UnitType => SchemaConfigConverters.unitConverter
          case StandardType.ByteType => ???
          case StandardType.StringType => ConfigWriter.stringConfigWriter
          case StandardType.BoolType => ConfigWriter.booleanConfigWriter
          case StandardType.ShortType => ConfigWriter.shortConfigWriter
          case StandardType.IntType => ConfigWriter.intConfigWriter
          case StandardType.LongType => ConfigWriter.longConfigWriter
          case StandardType.FloatType => ConfigWriter.floatConfigWriter
          case StandardType.DoubleType => ConfigWriter.doubleConfigWriter
          case StandardType.BinaryType => ???
          case StandardType.CharType => ConfigWriter.charConfigWriter
          case StandardType.BigIntegerType => ConfigWriter.bigIntegerWriter
          case StandardType.BigDecimalType => ConfigWriter.javaBigDecimalWriter
          case StandardType.UUIDType => ConfigWriter.uuidConfigWriter
          case StandardType.DayOfWeekType => ???
          case StandardType.DurationType => ConfigWriter.javaDurationConfigWriter
          case StandardType.InstantType => ConfigWriter.instantConfigWriter
          case StandardType.LocalDateType => ???
          case StandardType.LocalDateTimeType => ???
          case StandardType.LocalTimeType => ???
          // todo move it to object and assign as implicit val
          case StandardType.MonthType => ???
          case StandardType.MonthDayType => ???
          case StandardType.OffsetDateTimeType => ???
          case StandardType.OffsetTimeType => ???
          case StandardType.PeriodType => ConfigWriter.periodConfigWriter
          case StandardType.YearType => ConfigWriter.yearConfigWriter
          case StandardType.YearMonthType => ??? // ConfigReader.yearMo
          case StandardType.ZonedDateTimeType => ??? ///io.circe.Encoder[ZonedDateTime]
          case StandardType.ZoneIdType => ConfigWriter.zoneIdConfigWriter
          case StandardType.ZoneOffsetType => ConfigWriter.zoneOffsetConfigWriter

      addAnnotations(
        baseWriter.asInstanceOf[ConfigWriter[A]],
        SchemaAnnotations.extractAnnotations(typeId = None, annotations = annotations)
      )
    end primitiveConverter

    @nowarn
    private def addAnnotations[A](
      baseWriter: ConfigWriter[A],
      metadata: SchemaAnnotations[A]
    ): ConfigWriter[A] =
      baseWriter
  end Converter

end SchemaConfigWriterConverter
