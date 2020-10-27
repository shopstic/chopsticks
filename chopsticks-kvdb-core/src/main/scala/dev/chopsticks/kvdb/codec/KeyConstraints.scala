package dev.chopsticks.kvdb.codec

import cats.Show
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbUtils
import eu.timepit.refined.types.numeric.PosInt

import scala.collection.immutable.Queue

object KeyConstraints {
  type ConstraintsBuilder[K] = KeyConstraints[K] => KeyConstraints[K]
  type ConstraintsSeqBuilder[K] = KeyConstraints[K] => Seq[KeyConstraints[K]]
  type ConstraintsRangesBuilder[K] = KeyConstraints[K] => List[(KeyConstraints[K], KeyConstraints[K])]
  type ConstraintsRangesWithLimitBuilder[K] =
    KeyConstraints[K] => List[((KeyConstraints[K], KeyConstraints[K]), PosInt)]

  def seed[K: KeySerdes]: KeyConstraints[K] = KeyConstraints[K]()

  def first[K: KeySerdes]: KeyConstraints[K] = KeyConstraints[K](Queue(KvdbKeyConstraint(Operator.FIRST)))

  def last[K: KeySerdes]: KeyConstraints[K] = KeyConstraints[K](Queue(KvdbKeyConstraint(Operator.LAST)))

  def toList[K](constraints: KeyConstraints[K]): KvdbKeyConstraintList = {
    KvdbKeyConstraintList(constraints.constraints.toList)
  }

  def toRange[K](from: KeyConstraints[K], to: KeyConstraints[K], limit: Int = 0): KvdbKeyRange = {
    KvdbKeyRange(from.constraints.toList, to.constraints.toList, limit)
  }

  def constrain[K: KeySerdes](c: KeyConstraints[K] => KeyConstraints[K]): KvdbKeyConstraintList = {
    toList(c(seed))
  }

  def range[K: KeySerdes](
    from: KeyConstraints[K] => KeyConstraints[K],
    to: KeyConstraints[K] => KeyConstraints[K],
    limit: Int = 0
  ): KvdbKeyRange = {
    toRange(from(seed), to(seed), limit)
  }

  def build[K: KeySerdes](builder: ConstraintsBuilder[K]): KvdbKeyConstraintList = {
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

}

//noinspection ScalaStyle
// scalastyle:off
final case class KeyConstraints[K](constraints: Queue[KvdbKeyConstraint] = Queue.empty)(implicit serdes: KeySerdes[K]) {
  def gtEq[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(
        Operator.GREATER_EQUAL,
        ProtoByteString.copyFrom(serdes.serializePrefix(v)),
        v.toString
      )
    )
  }

  def >=[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = gtEq(v)

  def gt[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(
        Operator.GREATER,
        ProtoByteString.copyFrom(serdes.serializePrefix(v)),
        v.toString
      )
    )
  }

  def >[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = gt(v)

  def is(v: K): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(serdes.serialize(v)), v.toString)
    )
  }

  /*def is[A](v: A)(implicit k: KvdbKey.Aux[K, A :#: HNil], e: KvdbKey[A]): KvdbKeyConstraints[K] = {
    k.unused()
    copy(constraints enqueue KvdbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }*/

  def ltEq[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    copy(
      constraints enqueue KvdbKeyConstraint(
        Operator.LESS_EQUAL,
        ProtoByteString.copyFrom(serdes.serializePrefix(v)),
        v.toString
      )
    )
  }

  def <=[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = ltEq(v)

  def lt[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(
      Operator.LESS,
      ProtoByteString.copyFrom(serdes.serializePrefix(v)),
      v.toString
    ))
  }

  def <[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = lt(v)

  def lastStartsWith[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    val prefixBytes = serdes.serializePrefix(v)

    copy(
      constraints
        .enqueue(
          KvdbKeyConstraint(
            Operator.LESS,
            ProtoByteString.copyFrom(KvdbUtils.strinc(prefixBytes)),
            v.toString
          )
        )
        .enqueue(
          KvdbKeyConstraint(
            Operator.PREFIX,
            ProtoByteString.copyFrom(prefixBytes),
            v.toString
          )
        )
    )
  }

  def ^<=[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = lastStartsWith(v)

  def startsWith[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = {
    copy(constraints enqueue KvdbKeyConstraint(
      Operator.PREFIX,
      ProtoByteString.copyFrom(serdes.serializePrefix(v)),
      v.toString
    ))
  }

  def ^=[P](v: P)(implicit e: KeyPrefixEvidence[P, K]): KeyConstraints[K] = startsWith(v)

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
