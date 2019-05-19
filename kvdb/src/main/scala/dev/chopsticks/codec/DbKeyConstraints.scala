package dev.chopsticks.codec

import cats.Show
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import dev.chopsticks.proto.db.DbKeyConstraint.Operator
import dev.chopsticks.proto.db.{DbKeyConstraint, DbKeyConstraintList, DbKeyRange}

import scala.collection.immutable.Queue

object DbKeyConstraints {
  private val Seed: DbKeyConstraints[Any] = DbKeyConstraints[Any]()
  private val First: DbKeyConstraints[Any] = DbKeyConstraints[Any](Queue(DbKeyConstraint(Operator.FIRST)))
  private val Last: DbKeyConstraints[Any] = DbKeyConstraints[Any](Queue(DbKeyConstraint(Operator.LAST)))
  type ConstraintsBuilder[K] = DbKeyConstraints[K] => DbKeyConstraints[K]
  type ConstraintsSeqBuilder[K] = DbKeyConstraints[K] => Seq[DbKeyConstraints[K]]
  type ConstraintsRangesBuilder[K] = DbKeyConstraints[K] => List[(DbKeyConstraints[K], DbKeyConstraints[K])]

  def seed[K]: DbKeyConstraints[K] = Seed.asInstanceOf[DbKeyConstraints[K]]

  def first[K]: DbKeyConstraints[K] = First.asInstanceOf[DbKeyConstraints[K]]

  def last[K]: DbKeyConstraints[K] = Last.asInstanceOf[DbKeyConstraints[K]]

  def toList[K](constraints: DbKeyConstraints[K]): DbKeyConstraintList = {
    DbKeyConstraintList(constraints.constraints.toList)
  }

  def toRange[K](from: DbKeyConstraints[K], to: DbKeyConstraints[K]): DbKeyRange = {
    DbKeyRange(from.constraints.toList, to.constraints.toList)
  }

  def constrain[K](c: DbKeyConstraints[K] => DbKeyConstraints[K]): DbKeyConstraintList = {
    toList(c(seed))
  }

  def range[K](
    from: DbKeyConstraints[K] => DbKeyConstraints[K],
    to: DbKeyConstraints[K] => DbKeyConstraints[K]
  ): DbKeyRange = {
    toRange(from(seed), to(seed))
  }

  def build[K](builder: ConstraintsBuilder[K]): DbKeyConstraintList = {
    toList(builder(seed[K]))
  }

  object Implicits {
    implicit val dbKeyConstraintShow: Show[DbKeyConstraint] = Show.show { r =>
      val decodedDisplay = r.operandDisplay

      r.operator match {
        case Operator.GREATER => s"> $decodedDisplay"
        case Operator.GREATER_EQUAL => s">= $decodedDisplay"
        case Operator.LESS => s"< $decodedDisplay"
        case Operator.LESS_EQUAL => s"<= $decodedDisplay"
        case Operator.EQUAL => s"== $decodedDisplay"
        case Operator.PREFIX => s"^= $decodedDisplay"
        case Operator.FIRST => "FIRST"
        case Operator.LAST => "LAST"
        case _ => "UNKNOWN"
      }
    }

    implicit val dbKeyConstraintListShow: Show[List[DbKeyConstraint]] = Show.show { r =>
      r.map(_.show).mkString(" AND ")
    }

    implicit val dbKeyRangeShow: Show[DbKeyRange] = Show.show { r =>
      s"DbKeyRange(from ${r.from.show} -> to ${r.to.show})"
    }
  }

  private val MAX_BYTE = ProtoByteString.copyFrom(Array[Byte](0xFF.toByte))

}

//noinspection ScalaStyle
// scalastyle:off
final case class DbKeyConstraints[K](constraints: Queue[DbKeyConstraint] = Queue.empty) {
  def >=[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.GREATER_EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  def >[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  def is(v: K)(implicit e: DbKey[K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  /*def is[A](v: A)(implicit k: DbKey.Aux[K, A :#: HNil], e: DbKey[A]): DbKeyConstraints[K] = {
    k.unused()
    copy(constraints enqueue DbKeyConstraint(Operator.EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }*/

  def <=[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.LESS_EQUAL, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  def <[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.LESS, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  def ^<=[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(
      constraints enqueue DbKeyConstraint(
        Operator.LESS_EQUAL,
        ProtoByteString.copyFrom(e.encode(v)).concat(DbKeyConstraints.MAX_BYTE),
        v.toString
      )
    )
  }

  def ^=[P](v: P)(implicit e: DbKeyPrefix[P, K]): DbKeyConstraints[K] = {
    copy(constraints enqueue DbKeyConstraint(Operator.PREFIX, ProtoByteString.copyFrom(e.encode(v)), v.toString))
  }

  def first: DbKeyConstraints[K] = {
    assert(constraints.isEmpty, s"Calling first with a non-empty constraint list: $constraints")
    DbKeyConstraints.first
  }

  def last: DbKeyConstraints[K] = {
    assert(constraints.isEmpty, s"Calling last with a non-empty constraint list: $constraints")
    DbKeyConstraints.last
  }

  def infinity: DbKeyConstraints[K] = last
}
