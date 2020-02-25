package dev.chopsticks.avro4s

import com.sksamuel.avro4s.{AnnotationExtractors, Decoder, FieldMapper, NameExtractor}
import magnolia.{CaseClass, Magnolia, Param, SealedTrait, TypeName}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData, GenericEnumSymbol, IndexedRecord}
import shapeless.Refute

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap
import scala.language.experimental.macros
import scala.reflect.runtime.universe
@implicitNotFound(
  msg = "Implicit OptimizedDecoder[${T}] not found. Try supplying an implicit instance of OptimizedDecoder[${T}]"
)
trait OptimizedDecoder[T] extends Serializable {
  self =>

  def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T

  def map[U](fn: T => U): Decoder[U] =
    (value: Any, schema: Schema, fieldMapper: FieldMapper) => fn(self.decode(value, schema, fieldMapper))
}

trait LowPriorityOptimizedDecoder {
  implicit def decoderToOptimizedDecoder[A /*: Typeable*/ ](
    implicit e: Decoder[A],
    ev: Refute[Optimized[A]]
  ): OptimizedDecoder[A] = {
    import dev.chopsticks.util.implicits.UnusedImplicits._
    ev.unused()
    (value: Any, schema: Schema, fieldMapper: FieldMapper) => {
      e.decode(value, schema, fieldMapper)
    }
  }
}

object OptimizedDecoder extends LowPriorityOptimizedDecoder {
  type Typeclass[A] = OptimizedDecoder[A]

  def apply[T](implicit encoder: OptimizedDecoder[T]): OptimizedDecoder[T] = encoder

  private def tryDecode[T](fieldMapper: FieldMapper, schema: Schema, param: Param[Typeclass, T], value: Any) = {
    val decodeResult = util.Try {
      param.typeclass.decode(value, schema.getFields.get(param.index).schema, fieldMapper)
    }.toEither
    (decodeResult, param.default) match {
      case (Right(v), _) => v
      case (Left(_), Some(default)) => default
      case (Left(ex), _) => throw ex
    }
  }

  def combine[T](ctx: CaseClass[Typeclass, T]): OptimizedDecoder[T] = {
    if (ctx.isValueClass) {
      new OptimizedDecoder[T] {
        override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T = {
          val decoded = ctx.parameters.head.typeclass.decode(value, schema, fieldMapper)
          ctx.rawConstruct(List(decoded))
        }
      }
    }
    else {
      new OptimizedDecoder[T] {
        override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T = {
          value match {
            case record: IndexedRecord =>
              // if we are in here then we are decoding a case class so we need a record schema
              require(schema.getType == Schema.Type.RECORD)
              val values = ctx.parameters.map { p =>
                /**
                  * For a field in the target type (the case class we are marshalling to), we must
                  * try to pull a value from the Avro GenericRecord. After the value has been retrieved,
                  * it needs to be decoded into the appropriate Scala type.
                  *
                  * If the writer schema does not have an entry for the field then we can consider schema
                  * evolution using the following rules in the given order.
                  *
                  * 1. If the reader schema contains a default for this field, we will use that default.
                  * 2. If the parameter is defined with a scala default method then we will use that default value.
                  * 3. If the field is marked as @transient
                  *
                  * If none of these rules can be satisfied then an exception will be thrown.
                  */
                val extractor = new AnnotationExtractors(p.annotations)

                /**
                  * We may have a schema with a field in snake case like say { "first_name": "sam" } and
                  * that schema needs to be used for a case class field `firstName`.
                  * The field mapper is used to map fields in a schema to fields in a case class by
                  * transforming the class field name to the wire name format.
                  */
                val name = fieldMapper.to(extractor.name.getOrElse(p.label))

                // take into account @AvroName and use the field mapper to get the name of this parameter in the schema
                def field = record.getSchema.getField(name)

                // does the schema contain this parameter? If not, we will be relying on defaults or options in the case class
                if (extractor.transient || field == null) {
                  p.default match {
                    case Some(default) => default
                    // there is no default, so the field must be an option
                    case None => p.typeclass.decode(null, record.getSchema, fieldMapper)
                  }
                }
                else {
                  val k = record.getSchema.getFields.indexOf(field)
                  val value = record.get(k)
                  tryDecode(fieldMapper, schema, p, value)
                }
              }
              ctx.rawConstruct(values)
            case _: GenericData.EnumSymbol =>
              val res = ctx.parameters.map { p => tryDecode(fieldMapper, schema, p, value) }
              ctx.rawConstruct(res)
            case _ =>
              sys.error(
                s"This decoder can only handle types of EnumSymbol, IndexedRecord or it's subtypes such as GenericRecord [was ${value.getClass}]"
              )
          }
        }
      }
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): OptimizedDecoder[T] = {
    val enumMapCache = TrieMap.empty[TypeName, T]
    val nameCache = ctx.subtypes
      .foldLeft(Map.empty[(Schema.Type, TypeName), String]) { (m, subtype) =>
        m.updated(
            Schema.Type.RECORD -> subtype.typeName,
            NameExtractor(subtype.typeName, subtype.annotations ++ ctx.annotations).fullName
          )
          .updated(
            Schema.Type.UNION -> subtype.typeName,
            NameExtractor(subtype.typeName, subtype.annotations).fullName
          )
          .updated(Schema.Type.ENUM -> subtype.typeName, NameExtractor(subtype).name)
      }

    new OptimizedDecoder[T] {
      override def decode(container: Any, schema: Schema, fieldMapper: FieldMapper): T = {
        schema.getType match {
          case Schema.Type.RECORD =>
            container match {
              case container: GenericContainer =>
                val subtype = ctx.subtypes
                  .find { subtype =>
                    nameCache(Schema.Type.RECORD -> subtype.typeName) == container.getSchema.getFullName
                  }
                  .getOrElse(
                    sys.error(
                      s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"
                    )
                  )
                subtype.typeclass.decode(container, schema, fieldMapper)
              case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
            }
          // we have a union for nested ADTs and must extract the appropriate schema
          case Schema.Type.UNION =>
            container match {
              case container: GenericContainer =>
                val subschema = schema.getTypes.asScala
                  .find(_.getFullName == container.getSchema.getFullName)
                  .getOrElse(
                    sys.error(s"Could not find schema for ${container.getSchema.getFullName} in union schema $schema")
                  )
                val subtype = ctx.subtypes
                  .find { subtype =>
                    nameCache(Schema.Type.UNION -> subtype.typeName) == container.getSchema.getFullName
                  }
                  .getOrElse(
                    sys.error(
                      s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"
                    )
                  )
                subtype.typeclass.decode(container, subschema, fieldMapper)
              case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
            }
          // case objects are encoded as enums
          // we need to take the string and create the object
          case Schema.Type.ENUM =>
            val subtype = container match {
              case enum: GenericEnumSymbol[_] =>
                ctx.subtypes
                  .find { subtype => nameCache(Schema.Type.ENUM -> subtype.typeName) == enum.toString }
                  .getOrElse(sys.error(s"Could not find subtype for enum $enum"))
              case str: String =>
                ctx.subtypes
                  .find { subtype => nameCache(Schema.Type.ENUM -> subtype.typeName) == str }
                  .getOrElse(sys.error(s"Could not find subtype for enum $str"))
            }

            enumMapCache.getOrElseUpdate(
              subtype.typeName, {
                val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
                val module = runtimeMirror.staticModule(subtype.typeName.full)
                val companion = runtimeMirror.reflectModule(module.asModule)
                companion.instance.asInstanceOf[T]
              }
            )
          case other => sys.error(s"Unsupported sealed trait schema type $other")
        }
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
