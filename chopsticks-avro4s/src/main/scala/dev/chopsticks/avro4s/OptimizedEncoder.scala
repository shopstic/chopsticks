package dev.chopsticks.avro4s

import com.sksamuel.avro4s.Encoder.buildRecord
import com.sksamuel.avro4s._
import eu.timepit.refined.api.RefType
import magnolia._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import shapeless.Refute

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.language.higherKinds

@implicitNotFound(
  msg = "Implicit OptimizedEncoder[${T}] not found. Try supplying an implicit instance of OptimizedEncoder[${T}]"
)
trait OptimizedEncoder[T] extends Serializable {
  self =>
  def encode(t: T, schema: Schema, fieldMapper: FieldMapper): AnyRef
  def comap[S](fn: S => T): OptimizedEncoder[S] =
    (value: S, schema: Schema, fieldMapper: FieldMapper) => self.encode(fn(value), schema, fieldMapper)
}

trait LowPriorityOptimizedEncoder {
  implicit def encoderToOptimizedEncoder[A /*: Typeable*/ ](
    implicit e: Encoder[A],
    ev: Refute[Optimized[A]]
  ): OptimizedEncoder[A] = {
    import dev.chopsticks.util.implicits.UnusedImplicits._
    ev.unused()
    //    println("ORIGINAL: " + implicitly[Typeable[A]].describe)
    (t: A, schema: Schema, fieldMapper: FieldMapper) => e.encode(t, schema, fieldMapper)
  }
}

object OptimizedEncoder extends LowPriorityOptimizedEncoder {
  type Typeclass[A] = OptimizedEncoder[A]

  def apply[T](implicit encoder: OptimizedEncoder[T]): OptimizedEncoder[T] = encoder

  implicit def refinedEncoder[T: OptimizedEncoder, P, F[_, _]: RefType]: OptimizedEncoder[F[T, P]] = {
    (t: F[T, P], schema: Schema, fieldMapper: FieldMapper) =>
      implicitly[OptimizedEncoder[T]].encode(RefType[F].unwrap(t), schema, fieldMapper)
  }

  def combine[T](klass: CaseClass[Typeclass, T]): OptimizedEncoder[T] = {
    import scala.collection.JavaConverters._

    //    println("OPTIMIZED: " + klass.typeName)

    val nameExtractor = NameExtractor(klass.typeName, klass.annotations)
    val name = nameExtractor.name

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string
    if (klass.isValueClass) {
      new OptimizedEncoder[T] {
        override def encode(t: T, schema: Schema, fieldMapper: FieldMapper): AnyRef = {
          val p = klass.parameters.head
          p.typeclass.encode(p.dereference(t), schema, fieldMapper)
        }
      }
    }
    else {
      val defaultParamByNameMap = klass.parameters.map(p => (DefaultFieldMapper.to(p.label), p)).toMap

      new OptimizedEncoder[T] {
        override def encode(t: T, schema: Schema, fieldMapper: FieldMapper): AnyRef = {
          // the schema passed here must be a record since we are encoding a non-value case class
          require(schema.getType == Schema.Type.RECORD)
          val paramByNameMap =
            if (fieldMapper == DefaultFieldMapper) defaultParamByNameMap
            else klass.parameters.map(p => (fieldMapper.to(p.label), p)).toMap

          val values = schema.getFields.asScala.map { field =>
            // find the matching parameter
            val p = paramByNameMap.getOrElse(
              field.name(),
              sys.error(s"Could not find case class parameter for field ${field.name}")
            )
            p.typeclass.encode(p.dereference(t), field.schema, fieldMapper)
          }
          buildRecord(schema, values.toList, name)
        }
      }
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): OptimizedEncoder[T] = {
    //    println("DISPATCH: " + ctx.typeName)
    val nameCache = ctx.subtypes
      .foldLeft(Map.empty[(Schema.Type, TypeName), String]) { (m, subtype) =>
        m.updated(
            Schema.Type.UNION -> subtype.typeName,
            NameExtractor(subtype.typeName, subtype.annotations ++ ctx.annotations).fullName
          )
          .updated(Schema.Type.ENUM -> subtype.typeName, NameExtractor(subtype.typeName, subtype.annotations).name)
      }

    (t: T, schema: Schema, fieldMapper: FieldMapper) => {
      ctx.dispatch(t) { subtype =>
        schema.getType match {
          case Schema.Type.UNION =>
            val fullName = nameCache(Schema.Type.UNION -> subtype.typeName)
            val subschema = SchemaHelper.extractTraitSubschema(fullName, schema)
            subtype.typeclass.encode(t.asInstanceOf[subtype.SType], subschema, fieldMapper)

          case Schema.Type.ENUM =>
            val name = nameCache(Schema.Type.ENUM -> subtype.typeName)
            GenericData.get.createEnum(name, schema)
          case other =>
            sys.error(s"Unsupported schema type $other for sealed traits")
        }
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
