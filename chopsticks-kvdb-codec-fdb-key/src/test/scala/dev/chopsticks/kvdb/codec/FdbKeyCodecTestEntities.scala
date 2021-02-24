package dev.chopsticks.kvdb.codec

import enumeratum.EnumEntry
import enumeratum.values.{ByteEnum, ByteEnumEntry, IntEnum, IntEnumEntry}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import org.scalacheck.{Arbitrary, Gen}

object FdbKeyCodecTestEntities {
  final case class Sym(symbol: String) extends AnyVal

  sealed abstract class ByteEnumTest(val value: Byte) extends ByteEnumEntry

  object ByteEnumTest extends ByteEnum[ByteEnumTest] {
    final case object One extends ByteEnumTest(1)
    final case object Two extends ByteEnumTest(2)
    final case object Three extends ByteEnumTest(3)
    val values = findValues
  }

  implicit val byteEnumTestGen: Arbitrary[ByteEnumTest] = Arbitrary(Gen.oneOf(ByteEnumTest.values))

  sealed abstract class IntEnumTest(val value: Int) extends IntEnumEntry

  object IntEnumTest extends IntEnum[IntEnumTest] {
    final case object One extends IntEnumTest(1)
    final case object Two extends IntEnumTest(2)
    final case object Three extends IntEnumTest(3)
    val values = findValues
  }

  implicit val intEnumTestGen: Arbitrary[IntEnumTest] = Arbitrary(Gen.oneOf(IntEnumTest.values))

  sealed abstract class EnumTest(val value: String) extends EnumEntry

  object EnumTest extends enumeratum.Enum[EnumTest] {
    final case object One extends EnumTest("one")
    final case object Two extends EnumTest("one")
    final case object Three extends EnumTest("one")
    val values = findValues
  }

  implicit val enumTestGen: Arbitrary[EnumTest] = Arbitrary(Gen.oneOf(EnumTest.values))

  final case class ContravariantKeyPrefixTest(foo: EnumTest, bar: IntEnumTest, baz: ByteEnumTest)
  object ContravariantKeyPrefixTest {
    import dev.chopsticks.kvdb.codec.fdb_key._
    implicit val dbKey = KeySerdes[ContravariantKeyPrefixTest]
    implicit val dbKeyPrefix = KeyPrefix[(EnumTest, IntEnumTest), ContravariantKeyPrefixTest]
  }

  final case class TestKeyWithRefined(foo: NonEmptyString, bar: PortNumber)
  object TestKeyWithRefined {
    import dev.chopsticks.kvdb.codec.fdb_key._
    implicit val dbKey = KeySerdes[TestKeyWithRefined]
  }

  type NonEmptyStringAlias = Refined[String, NonEmpty]
  final case class TestKeyWithRefinedAlias(bar: PortNumber, foo: NonEmptyStringAlias)

  sealed trait SealedTraitTest
  final case class SealedTraitTestFoo(foo: Int) extends SealedTraitTest
  final case class SealedTraitTestBar(bar: Int) extends SealedTraitTest
  final case class SealedTraitTestBaz(baz: Int) extends SealedTraitTest
  final case class SealedTraitTestBoo(boom: Int) extends SealedTraitTest

  object SealedTraitTest {
    implicit val tag = FdbKeyCoproductTag[SealedTraitTest, String](_.drop("SealedTraitTest".length))
  }

  final case class KeyWithSealedTraitTest(param: SealedTraitTest)

  object KeyWithSealedTraitTest {
    import dev.chopsticks.kvdb.codec.fdb_key._
    implicit val dbKey = KeySerdes[KeyWithSealedTraitTest]
  }
}
