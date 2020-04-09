package dev.chopsticks.kvdb.codec

import cats.Show
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}

import scala.collection.immutable.Queue

object KeyConstraints {
  private val Seed: KeyConstraints[Any] = KeyConstraints[Any]()
  private val First: KeyConstraints[Any] = KeyConstraints[Any](Queue(KvdbKeyConstraint(Operator.FIRST)))
  private val Last: KeyConstraints[Any] = KeyConstraints[Any](Queue(KvdbKeyConstraint(Operator.LAST)))
  type ConstraintsBuilder[K] = KeyConstraints[K] => KeyConstraints[K]
  type ConstraintsSeqBuilder[K] = KeyConstraints[K] => Seq[KeyConstraints[K]]
  type ConstraintsRangesBuilder[K] = KeyConstraints[K] => List[(KeyConstraints[K], KeyConstraints[K])]

  def seed[K]: KeyConstraints[K] = Seed.asInstanceOf[KeyConstraints[K]]

  def first[K]: KeyConstraints[K] = First.asInstanceOf[KeyConstraints[K]]

  def last[K]: KeyConstraints[K] = Last.asInstanceOf[KeyConstraints[K]]

  def toList[K](constraints: KeyConstraints[K]): KvdbKeyConstraintList = {
    KvdbKeyConstraintList(constraints.constraints.toList)
  }

  def toRange[K](from: KeyConstraints[K], to: KeyConstraints[K]): KvdbKeyRange = {
    KvdbKeyRange(from.constraints.toList, to.constraints.toList)
  }

  def constrain[K](c: KeyConstraints[K] => KeyConstraints[K]): KvdbKeyConstraintList = {
    toList(c(seed))
  }

  def range[K](
    from: KeyConstraints[K] => KeyConstraints[K],
    to: KeyConstraints[K] => KeyConstraints[K]
  ): KvdbKeyRange = {
    toRange(from(seed), to(seed))
  }

  def build[K](builder: ConstraintsBuilder[K]): KvdbKeyConstraintList = {
    toList(builder(seed[K]))
  }

  object Implicits {
    implicit val dbKeyConstraintShow: Show[KvdbKeyConstraint] = Show.show { r =>
      val decodedDisplay = r.operandDisplay

      r.operator match {
        case Operator.GREATER => s"GREATER_THAN $decodedDisplay"
        case Operator.GREATER_EQUAL => s"GREATER_OR_EQUAL $decodedDisplay"
        case Operator.LESS => s"LESS_THAN $decodedDisplay"
        case Operator.LESS_EQUAL => s"LESS_OR_EQUAL $decodedDisplay"
        case Operator.EQUAL => s"== $decodedDisplay"
        case Operator.PREFIX => s"STARTS_WITH $decodedDisplay"
        case Operator.FIRST => "FIRST"
        case Operator.LAST => "LAST"
        case _ => "UNKNOWN"
      }
    }

    implicit val dbKeyConstraintListShow: Show[List[KvdbKeyConstraint]] = Show.show { r =>
      r.map(_.show).mkString(" AND ")
    }

    implicit val dbKeyRangeShow: Show[KvdbKeyRange] = Show.show { r =>
      s"KvdbKeyRange(from ${r.from.show} -> to ${r.to.show})"
    }
  }

  private val MAX_BYTE = ProtoByteString.copyFrom(Array[Byte](0xFF.toByte))
}

//noinspection ScalaStyle
// scalastyle:off
final case class KeyConstraints[K](constraints: Queue[KvdbKeyConstraint] = Queue.empty) {
  def >=[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(
        Operator.GREATER_EQUAL,
        ProtoByteString.copyFrom(e.serialize(v)),
        v.toString
      )
    )
  }

  def gtEq[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = >=(v)

  def >[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(e.serialize(v)), v.toString))
  }

  def gt[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = >(v)

  def is(v: K)(implicit e: KeySerdes[K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(e.serialize(v)), v.toString))
  }

  /*def is[A](v: A)(implicit k: KvdbKey.Aux[K, A :#: HNil], e: KvdbKey[A]): KvdbKeyConstraints[K] = {
    k.unused()
    copy(constraints enqueue KvdbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }*/

  def <=[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(Operator.LESS_EQUAL, ProtoByteString.copyFrom(e.serialize(v)), v.toString)
    )
  }

  def ltEq[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = <=(v)

  def <[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(Operator.LESS, ProtoByteString.copyFrom(e.serialize(v)), v.toString))
  }

  def lt[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = <(v)

  def ^<=[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(
        Operator.LESS_EQUAL,
        ProtoByteString.copyFrom(e.serialize(v)).concat(KeyConstraints.MAX_BYTE),
        v.toString
      )
    )
  }

  def ^=[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(Operator.PREFIX, ProtoByteString.copyFrom(e.serialize(v)), v.toString))
  }

  def startsWith[P](v: P)(implicit e: KeyPrefix[P, K]): KeyConstraints[K] = ^=(v)

  def first: KeyConstraints[K] = {
    assert(constraints.isEmpty, s"Calling first with a non-empty constraint list: $constraints")
    KeyConstraints.first
  }

  def last: KeyConstraints[K] = {
    assert(constraints.isEmpty, s"Calling last with a non-empty constraint list: $constraints")
    KeyConstraints.last
  }

  def infinity: KeyConstraints[K] = last
}
