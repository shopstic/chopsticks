package dev.chopsticks.kvdb.codec

import java.time.{LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import dev.chopsticks.kvdb.codec.BerkeleydbKeyCodecTest.TestKeyWithRefined
import dev.chopsticks.testkit.ArbitraryTime._
import enumeratum.EnumEntry
import enumeratum.values.{ByteEnum, ByteEnumEntry, IntEnum, IntEnumEntry}
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import org.scalacheck.{Arbitrary, Gen}
import org.scalactic.anyvals.{PosInt, PosZDouble}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

object BerkeleydbKeyCodecTest {
  final case class Sym(symbol: String) extends AnyVal

  sealed abstract class ByteEnumTest(val value: Byte) extends ByteEnumEntry

  object ByteEnumTest extends ByteEnum[ByteEnumTest] {
    case object One extends ByteEnumTest(1)
    case object Two extends ByteEnumTest(2)
    case object Three extends ByteEnumTest(3)
    val values = findValues
  }

  implicit val byteEnumTestGen: Arbitrary[ByteEnumTest] = Arbitrary(Gen.oneOf(ByteEnumTest.values))

  sealed abstract class IntEnumTest(val value: Int) extends IntEnumEntry

  object IntEnumTest extends IntEnum[IntEnumTest] {
    case object One extends IntEnumTest(1)
    case object Two extends IntEnumTest(2)
    case object Three extends IntEnumTest(3)
    val values = findValues
  }

  implicit val intEnumTestGen: Arbitrary[IntEnumTest] = Arbitrary(Gen.oneOf(IntEnumTest.values))

  sealed abstract class EnumTest(val value: String) extends EnumEntry

  object EnumTest extends enumeratum.Enum[EnumTest] {
    case object One extends EnumTest("one")
    case object Two extends EnumTest("one")
    case object Three extends EnumTest("one")
    val values = findValues
  }

  implicit val enumTestGen: Arbitrary[EnumTest] = Arbitrary(Gen.oneOf(EnumTest.values))

  final case class ContravariantKeyPrefixTest(foo: EnumTest, bar: IntEnumTest, baz: ByteEnumTest)
  object ContravariantKeyPrefixTest {
    import dev.chopsticks.kvdb.codec.berkeleydb_key._
    implicit val dbKey = KeySerdes[ContravariantKeyPrefixTest]
    implicit val dbKeyPrefix = KeyPrefixEvidence[(EnumTest, IntEnumTest), ContravariantKeyPrefixTest]
  }

  final case class TestKeyWithRefined(foo: NonEmptyString, bar: PortNumber)
  object TestKeyWithRefined {
    import dev.chopsticks.kvdb.codec.berkeleydb_key._
    implicit val dbKey = KeySerdes[TestKeyWithRefined]
  }
}

//noinspection TypeAnnotation
class BerkeleydbKeyCodecTest extends AnyWordSpecLike with Assertions with Matchers with ScalaCheckDrivenPropertyChecks {
  "Scala tuple" should {
    import berkeleydb_key._

    type TupleKey = (Boolean, Option[Int], String)
    implicit val tupleDbKey = KeySerdes[TupleKey]

    type NestedKey = (Int, ((Boolean, (Long, LocalDate), String), Double))
    implicit val nestedDbKey = KeySerdes[NestedKey]

    type FlattenedKey = (Int, Boolean, Long, LocalDate, String, Double)
    implicit val flattenedDbKey = KeySerdes[FlattenedKey]

    "serialize and deserialize correctly" in {
      val key: TupleKey = (true, Some(123), "cool")
      assert(KeySerdes.deserialize[TupleKey](KeySerdes.serialize(key)) == Right(key))
    }

    "treat nested tuple the same with flattened version" in {
      val nested: FlattenedKey = (123, true, 500L, LocalDate.of(2018, 1, 2), "foo bar", 99.887d)
      val flattened: NestedKey = (123, ((true, (500L, LocalDate.of(2018, 1, 2)), "foo bar"), 99.887d))

      assert(KeySerdes.isEqual(KeySerdes.serialize(nested), KeySerdes.serialize(flattened)))
    }
  }

  "case class" should {
    case class Address(street: String, zip: Int)

    case class Person(
      name: String,
      address: Address,
      age: Short,
      balance: Option[BigDecimal],
      isCool: Boolean,
      dob: LocalDate,
      lastSeen: LocalDateTime,
      health: Double
    )
    object Person {
      import berkeleydb_key._
      implicit val dbKey = KeySerdes[Person]
    }

    "serialize and deserialize correctly" in {
      val person1 = Person(
        name = "Jacky",
        address = Address("Portstewart Dr", 34202),
        age = 31,
        balance = Some(BigDecimal("12345678.64345")),
        isCool = true,
        dob = LocalDate.of(1987, 8, 3),
        lastSeen = LocalDateTime.now,
        health = 89.95d
      )
      assert(KeySerdes.deserialize[Person](KeySerdes.serialize(person1)) == Right(person1))

      val person2 = Person(
        name = "Abe",
        address = Address("Waters Edge", 34202),
        age = 40,
        balance = None,
        isCool = true,
        dob = LocalDate.MIN,
        lastSeen = LocalDateTime.now,
        health = 95.67d
      )
      assert(KeySerdes.deserialize[Person](KeySerdes.serialize(person2)) == Right(person2))
    }
  }

  "prefix" should {
    import BerkeleydbKeyCodecTest.Sym
    case class TradeTick(symbol: Sym, dateTime: LocalDateTime, price: BigDecimal, size: Int)
    object TradeTick {
      import berkeleydb_key._
      implicit val dbKey = KeySerdes[TradeTick]
    }

    case class Prefix(symbol: Sym, dateTime: LocalDateTime)
    object Prefix {
      implicit val dbKeyPrefix = KeyPrefixEvidence[Prefix, TradeTick]
    }

    "serialize" in {
      val now = LocalDateTime.now

      val tick = TradeTick(Sym("AAPL"), now, BigDecimal("1234.67"), 1000)
      val prefix = Prefix(Sym("AAPL"), now)
      assert(KeySerdes.isPrefix(KeySerdes[TradeTick].serializePrefix(prefix), KeySerdes.serialize(tick)))
    }
  }

  "legacy literal string keys" should {
    import dev.chopsticks.kvdb.codec.primitive._
    "compile for KeyPrefixEvidence" in {
      implicitly[KeyPrefixEvidence[String, String]]
    }

    "compile for KeySerdes" in {
      implicitly[KeySerdes[String]]
    }

    "not implicitly compile for Product, we want to enforce it explicitly" in {
      assertDoesNotCompile("""
                             |implicitly[KeySerdes[Tuple1[String]]]
        """.stripMargin)
    }
  }

//  "different codecs between the key and its prefix" should {
//    final case class TestKey(foo: String)
//    "not compile" in {
//      assertDoesNotCompile("""
//                             |implicit val oneDbKey = {
//                             |  import dev.chopsticks.kvdb.codec.berkeleydb_key._
//                             |  KeySerdes[TestKey]
//                             |}
//                             |import dev.chopsticks.kvdb.codec.primitive._
//                             |implicitly[KeyPrefixEvidence[String, TestKey]]
//        """.stripMargin)
//    }
//  }

  "compare" when {
    implicit val generatorDrivenConfig =
      PropertyCheckConfiguration(minSuccessful = PosInt(100), maxDiscardedFactor = PosZDouble(10))

    "LocalDate" should {
      case class DateKey(date: LocalDate)
      object DateKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[DateKey]
      }

      "compare < 0" in {
        forAll { (a: LocalDate, b: LocalDate) =>
          whenever(a.isBefore(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(DateKey(a)), KeySerdes.serialize(DateKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalDate =>
          assert(KeySerdes.compare(KeySerdes.serialize(DateKey(a)), KeySerdes.serialize(DateKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalDate, b: LocalDate) =>
          whenever(a.isAfter(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(DateKey(a)), KeySerdes.serialize(DateKey(b))) > 0)
          }
        }
      }
    }

    "LocalDateTime" should {
      case class DateTimeKey(date: LocalDateTime)
      object DateTimeKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[DateTimeKey]
      }

      "compare < 0" in {
        forAll { (a: LocalDateTime, b: LocalDateTime) =>
          whenever(a.isBefore(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(DateTimeKey(a)), KeySerdes.serialize(DateTimeKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalDateTime =>
          assert(KeySerdes.compare(KeySerdes.serialize(DateTimeKey(a)), KeySerdes.serialize(DateTimeKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalDateTime, b: LocalDateTime) =>
          whenever(a.isAfter(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(DateTimeKey(a)), KeySerdes.serialize(DateTimeKey(b))) > 0)
          }
        }
      }
    }

    "BigDecimal" should {
      case class BigDecimalKey(value: BigDecimal)
      object BigDecimalKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[BigDecimalKey]
      }

      "compare < 0" in {
        forAll { (a: BigDecimal, b: BigDecimal) =>
          whenever(a < b) {
            assert(KeySerdes.compare(KeySerdes.serialize(BigDecimalKey(a)), KeySerdes.serialize(BigDecimalKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: BigDecimal =>
          assert(KeySerdes.compare(KeySerdes.serialize(BigDecimalKey(a)), KeySerdes.serialize(BigDecimalKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: BigDecimal, b: BigDecimal) =>
          whenever(a > b) {
            assert(KeySerdes.compare(KeySerdes.serialize(BigDecimalKey(a)), KeySerdes.serialize(BigDecimalKey(b))) > 0)
          }
        }
      }
    }

    "LocalTime" should {
      case class LocalTimeKey(value: LocalTime)
      object LocalTimeKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[LocalTimeKey]
      }

      "compare < 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isBefore(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(LocalTimeKey(a)), KeySerdes.serialize(LocalTimeKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalTime =>
          assert(KeySerdes.compare(KeySerdes.serialize(LocalTimeKey(a)), KeySerdes.serialize(LocalTimeKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isAfter(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(LocalTimeKey(a)), KeySerdes.serialize(LocalTimeKey(b))) > 0)
          }
        }
      }
    }

    "Option" should {
      case class LocalTimeKey(value: Option[LocalTime])
      object LocalTimeKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[LocalTimeKey]
      }

      "compare < 0" in {
        forAll { b: LocalTime =>
          assert(
            KeySerdes.compare(KeySerdes.serialize(LocalTimeKey(None)), KeySerdes.serialize(LocalTimeKey(Some(b)))) < 0
          )
        }
      }

      "compare > 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isAfter(b)) {
            assert(
              KeySerdes
                .compare(KeySerdes.serialize(LocalTimeKey(Some(a))), KeySerdes.serialize(LocalTimeKey(Some(b)))) > 0
            )
          }
        }
      }
    }

    "YearMonth" should {
      case class YearMonthKey(value: YearMonth)
      object YearMonthKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[YearMonthKey]
      }

      "compare < 0" in {
        forAll { (a: YearMonth, b: YearMonth) =>
          whenever(a.isBefore(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(YearMonthKey(a)), KeySerdes.serialize(YearMonthKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: YearMonth =>
          assert(KeySerdes.compare(KeySerdes.serialize(YearMonthKey(a)), KeySerdes.serialize(YearMonthKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: YearMonth, b: YearMonth) =>
          whenever(a.isAfter(b)) {
            assert(KeySerdes.compare(KeySerdes.serialize(YearMonthKey(a)), KeySerdes.serialize(YearMonthKey(b))) > 0)
          }
        }
      }
    }

    "Int" should {
      case class IntKey(value: Int)
      object IntKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[IntKey]
      }

      "compare < 0" in {
        forAll { (a: Int, b: Int) =>
          whenever(a < b) {
            assert(KeySerdes.compare(KeySerdes.serialize(IntKey(a)), KeySerdes.serialize(IntKey(b))) < 0)
          }
        }
      }

      "compare > 0" in {
        forAll { (a: Int, b: Int) =>
          whenever(a > b) {
            assert(KeySerdes.compare(KeySerdes.serialize(IntKey(a)), KeySerdes.serialize(IntKey(b))) > 0)
          }
        }
      }
    }

    "Long" should {
      case class LongKey(value: Long)
      object LongKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[LongKey]
      }

      "compare < 0" in {
        forAll { (a: Long, b: Long) =>
          whenever(a < b) {
            assert(KeySerdes.compare(KeySerdes.serialize(LongKey(a)), KeySerdes.serialize(LongKey(b))) < 0)
          }
        }
      }

      "compare > 0" in {
        forAll { (a: Long, b: Long) =>
          whenever(a > b) {
            assert(KeySerdes.compare(KeySerdes.serialize(LongKey(a)), KeySerdes.serialize(LongKey(b))) > 0)
          }
        }
      }
    }

    "Boolean" should {
      case class BooleanKey(value: Boolean)
      object BooleanKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[BooleanKey]
      }

      "compare" in {
        assert(KeySerdes.compare(KeySerdes.serialize(BooleanKey(false)), KeySerdes.serialize(BooleanKey(true))) < 0)
        assert(KeySerdes.compare(KeySerdes.serialize(BooleanKey(true)), KeySerdes.serialize(BooleanKey(true))) == 0)
        assert(KeySerdes.compare(KeySerdes.serialize(BooleanKey(false)), KeySerdes.serialize(BooleanKey(false))) == 0)
      }
    }

    "UUID" should {
      case class UuidKey(value: UUID, foo: Boolean)
      object UuidKey {
        import berkeleydb_key._
        implicit val dbKey = KeySerdes[UuidKey]
      }
      "serialize / deserialize" in {
        forAll { (uuid: UUID, foo: Boolean) =>
          val key = UuidKey(uuid, foo)
          KeySerdes.deserialize[UuidKey](KeySerdes.serialize(key)) should equal(Right(key))
        }
      }
    }

    "Enumeratum" when {
      import BerkeleydbKeyCodecTest._

      "ByteEnum" should {
        "serialize / deserialize" in {
          forAll { (entry: ByteEnumTest) =>
            import berkeleydb_key._
            KeySerdes.deserialize[ByteEnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }

      "IntEnum" should {
        "serialize / deserialize" in {
          forAll { (entry: IntEnumTest) =>
            import berkeleydb_key._
            KeySerdes.deserialize[IntEnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }

      "Enum" should {
        "serialize / deserialize" in {
          forAll { (entry: EnumTest) =>
            import berkeleydb_key._
            KeySerdes.deserialize[EnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }
    }

    "refined" should {
      "serdes" in {
        import berkeleydb_key._
        import eu.timepit.refined.auto._
        val key = TestKeyWithRefined("foo", 1234)
        KeySerdes.deserialize[TestKeyWithRefined](KeySerdes.serialize(key)) should equal(Right(key))
      }
    }

    "KeyPrefixEvidence" should {
      "be contravariant" in {
        import BerkeleydbKeyCodecTest._
        KeyPrefixEvidence[(EnumTest.One.type, IntEnumTest.Two.type), ContravariantKeyPrefixTest] should be(
          ContravariantKeyPrefixTest.dbKeyPrefix
        )
      }
    }
  }
}
