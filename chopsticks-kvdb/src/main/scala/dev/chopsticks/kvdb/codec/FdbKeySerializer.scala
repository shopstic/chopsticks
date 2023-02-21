package dev.chopsticks.kvdb.codec

import java.time.*
import java.util.UUID
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import enumeratum.EnumEntry
import enumeratum.values.{ByteEnumEntry, IntEnumEntry, ShortEnumEntry}
import magnolia1.*
import scalapb.GeneratedEnum

import scala.annotation.implicitNotFound
import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

@implicitNotFound(
  msg = "Implicit FdbKeySerializer[${K}] not found. Try supplying an implicit instance of FdbKeySerializer[${K}]"
)
trait FdbKeySerializer[K] extends KeySerializer[K] with KeyPrefixSerializer[K]:
  def serialize(o: Tuple, t: K): Tuple
  def serializePrefix(tupleOutput: Tuple, prefix: scala.Tuple): (Tuple, scala.Tuple)
  final def serialize(key: K): Array[Byte] =
    val tuple = serialize(new Tuple(), key)
    if (tuple.hasIncompleteVersionstamp) tuple.packWithVersionstamp()
    else tuple.pack()
  final override def serializePrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]): Array[Byte] =
    val flattened = ev.flatten(prefix)
    serializePrefix(new Tuple(), flattened) match
      case (output, EmptyTuple) =>
        if (output.hasIncompleteVersionstamp) output.packWithVersionstamp()
        else output.pack()
      case (_, leftover) =>
        throw new IllegalStateException(
          s"Prefix has not been fully consumed during serialization. Leftovers: $leftover"
        )
  final override def serializeLiteralPrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]): Array[Byte] =
    val serialized = serializePrefix[Prefix](prefix)
    if (serialized.isEmpty || serialized.last != 0) serialized
    else
      val copy = new Array[Byte](serialized.length - 1)
      System.arraycopy(serialized, 0, copy, 0, serialized.length - 1)
      copy

end FdbKeySerializer

trait PredefinedFdbKeySerializer[T] extends FdbKeySerializer[T]
trait CaseClassFdbKeySerializer[T] extends FdbKeySerializer[T]
trait SealedTraitFdbKeySerializer[T] extends FdbKeySerializer[T]

object FdbKeySerializer extends Derivation[FdbKeySerializer]:
  type Typeclass[A] = FdbKeySerializer[A]

  implicit val stringFdbKeyEncoder: PredefinedFdbKeySerializer[String] = define((o, v) => o.add(v))
  implicit val booleanFdbKeyEncoder: PredefinedFdbKeySerializer[Boolean] = define((o, v) => o.add(v))
  implicit val byteFdbKeyEncoder: PredefinedFdbKeySerializer[Byte] = define((o, v) => o.add(v.toLong))
  implicit val shortFdbKeyEncoder: PredefinedFdbKeySerializer[Short] = define((o, v) => o.add(v.toLong))
  implicit val intFdbKeyEncoder: PredefinedFdbKeySerializer[Int] = define((o, v) => o.add(v.toLong))
  implicit val longFdbKeyEncoder: PredefinedFdbKeySerializer[Long] = define((o, v) => o.add(v))
  implicit val doubleFdbKeyEncoder: PredefinedFdbKeySerializer[Double] = define((o, v) => o.add(v))
  implicit val floatFdbKeyEncoder: PredefinedFdbKeySerializer[Float] = define((o, v) => o.add(v))

  implicit val byteArraySeqFdbKeyEncoder: PredefinedFdbKeySerializer[ArraySeq[Byte]] =
    define((o, v) => o.add(v.unsafeArray.asInstanceOf[Array[Byte]]))
  implicit val byteStringFdbKeyEncoder: PredefinedFdbKeySerializer[ByteString] =
    define((o, v) => o.add(v.toByteArray))
  implicit val ldFdbKeyEncoder: PredefinedFdbKeySerializer[LocalDate] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.toEpochDay))
  implicit val instantFdbKeyEncoder: PredefinedFdbKeySerializer[Instant] = define { (o, v) =>
    o.add(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltFdbKeyEncoder: PredefinedFdbKeySerializer[LocalTime] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.toNanoOfDay))
  implicit val ymFdbKeyEncoder: PredefinedFdbKeySerializer[YearMonth] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.getYear.toLong * 100 + v.getMonthValue))
  implicit val uuidFdbKeyEncoder: PredefinedFdbKeySerializer[UUID] =
    define((o, v) => o.add(v))
  implicit val versionStampFdbKeyEncoder: PredefinedFdbKeySerializer[Versionstamp] =
    define((o, v) => o.add(v))

  given protobufEnumFdbKeyEncoder[T <: GeneratedEnum: ClassTag]: PredefinedFdbKeySerializer[T] =
    define((o, v) => intFdbKeyEncoder.serialize(o, v.value))

  given enumeratumByteEnumKeyEncoder[E <: ByteEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  given enumeratumShortEnumKeyEncoder[E <: ShortEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  given enumeratumIntEnumKeyEncoder[E <: IntEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  given enumeratumEnumKeyEncoder[E <: EnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.entryName))

//  implicit def refinedFdbKeySerializer[F[_, _], T, P](implicit
//    serializer: FdbKeySerializer[T],
//    refType: RefType[F],
//    @nowarn validate: Validate[T, P],
//    @nowarn ct: ClassTag[F[T, P]]
//  ): PredefinedFdbKeySerializer[F[T, P]] = {
//    define((o: Tuple, t: F[T, P]) => serializer.serialize(o, refType.unwrap(t)))
//  }

  def apply[V](implicit f: FdbKeySerializer[V]): FdbKeySerializer[V] = f

  def define[T](ser: (Tuple, T) => Tuple)(using ct: ClassTag[T]): PredefinedFdbKeySerializer[T] =
    new PredefinedFdbKeySerializer[T]:
      override def serialize(o: Tuple, t: T): Tuple =
        ser(o, t)
      override def serializePrefix(tupleOutput: Tuple, prefix: scala.Tuple): (Tuple, scala.Tuple) =
        prefix match
          case (head: T) *: tail =>
            (serialize(tupleOutput, head), tail)
          case _ =>
            throw new IllegalStateException(s"Invalid prefix: $prefix for ${ct.toString()}")

  override def join[T](ctx: CaseClass[FdbKeySerializer, T]): CaseClassFdbKeySerializer[T] =
    new CaseClassFdbKeySerializer[T]:
      override def serialize(tupleOutput: Tuple, value: T): Tuple =
        ctx.params.foldLeft(tupleOutput) { (tuple, p) => p.typeclass.serialize(tuple, p.deref(value)) }
      override def serializePrefix(tupleOutput: Tuple, prefix: scala.Tuple): (Tuple, scala.Tuple) =
        // Deliberately optimized for simplicity & best performance
        var result = (tupleOutput, prefix)
        var done = false
        val params = ctx.params.iterator
        while (!done && params.hasNext) {
          result match {
            case (output, remaining) =>
              if (remaining == EmptyTuple) {
                done = true
              }
              else {
                result = params.next().typeclass.serializePrefix(output, remaining)
              }
          }
        }
        result

  override def split[A](ctx: SealedTrait[FdbKeySerializer, A]): SealedTraitFdbKeySerializer[A] =
    ???
//
//  def split[A, Tag](ctx: SealedTrait[FdbKeySerializer, A])(implicit
//    tag: FdbKeyCoproductTag.Aux[A, Tag],
//    tagSerializer: FdbKeySerializer[Tag]
//  ): SealedTraitFdbKeySerializer[A] =
//    new SealedTraitFdbKeySerializer[A]:
//      override def serialize(tuple: Tuple, value: A): Tuple =
//        ctx.choose(value) { (subType: ctx.Subtype[_]) =>
//          subType.typeclass.serialize(
//            tagSerializer.serialize(tuple, tag.subTypeToTag(subType)),
//            subType.cast(value)
//          )
//        }

  //noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit encoder: FdbKeySerializer[T]): PredefinedFdbKeySerializer[Option[T]] = {
    define { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.serialize(o.add(true), v)
        case None => o.add(false)
      }
    }
  }

//  implicit def deriveSerializer[A]: FdbKeySerializer[A] = macro Magnolia.gen[A]
end FdbKeySerializer
