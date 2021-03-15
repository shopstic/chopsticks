package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.ValueSerdes
import dev.chopsticks.kvdb.codec.protobuf_value.test_proto.{Expr, Literal, Mul}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

final class ProtobufValueSerdesTest extends AnyWordSpecLike with Assertions with Matchers {
  "Sealed Oneof" should {
    "serdes correctly for simple coproduct" in {
      import dev.chopsticks.kvdb.codec.protobuf_value._

      val valueSerdes = ValueSerdes[Expr]
      val value = Literal(123)

      valueSerdes.deserialize(valueSerdes.serialize(value)) should equal(Right(value))
    }

    "serdes correctly for recursive coproduct" in {
      import dev.chopsticks.kvdb.codec.protobuf_value._

      val valueSerdes = ValueSerdes[Expr]
      val value = Mul(Literal(123), Literal(456))

      valueSerdes.deserialize(valueSerdes.serialize(value)) should equal(Right(value))
    }
  }
}
