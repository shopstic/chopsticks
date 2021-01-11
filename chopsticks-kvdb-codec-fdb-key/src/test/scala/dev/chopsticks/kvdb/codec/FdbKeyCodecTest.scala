package dev.chopsticks.kvdb.codec

import java.time.{LocalDate, LocalDateTime, LocalTime, YearMonth, ZoneId}
import java.util.UUID
import dev.chopsticks.kvdb.codec.FdbKeyCodecTestEntities._
import dev.chopsticks.testkit.ArbitraryTime._
import eu.timepit.refined.types.string.NonEmptyString
import org.scalactic.anyvals.{PosInt, PosZDouble}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

object FdbKeyCodecTest {
  //noinspection TypeAnnotation
  implicit lazy val defaultZoneIdLocalDateTimeKeySerdes =
    FdbKeySerdes.createLocalDateTimeKeySerdes(ZoneId.systemDefault())
}

class FdbKeyCodecTest extends AnyWordSpecLike with Assertions with Matchers with ScalaCheckDrivenPropertyChecks {

  "Scala tuple" should {
    import fdb_key._

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
      balance: Option[Double],
      isCool: Boolean,
      dob: LocalDate,
      lastSeen: LocalDateTime,
      health: Double
    )
    object Person {
      import fdb_key._
      import FdbKeyCodecTest.defaultZoneIdLocalDateTimeKeySerdes
      implicit val dbKey = KeySerdes[Person]
    }

    "serialize and deserialize correctly" in {
      val person1 = Person(
        name = "Jacky",
        address = Address("Portstewart Dr", 34202),
        age = 31,
        balance = Some(12345678.64345d),
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
    case class TradeTick(symbol: Sym, dateTime: LocalDateTime, price: Double, size: Int)
    object TradeTick {
      import fdb_key._
      import FdbKeyCodecTest.defaultZoneIdLocalDateTimeKeySerdes
      implicit val dbKey = KeySerdes[TradeTick]
    }

    case class Prefix(symbol: Sym, dateTime: LocalDateTime)
    object Prefix {
      implicit val dbKeyPrefix = KeyPrefix[Prefix, TradeTick]
    }

    "serialize" in {
      val now = LocalDateTime.now

      val tick = TradeTick(Sym("AAPL"), now, 1234.67d, 1000)
      val prefix = Prefix(Sym("AAPL"), now)
      assert(KeySerdes.isPrefix(KeySerdes[TradeTick].serializePrefix(prefix), KeySerdes.serialize(tick)))
    }
  }

  "compare" when {
    implicit val generatorDrivenConfig =
      PropertyCheckConfiguration(minSuccessful = PosInt(100), maxDiscardedFactor = PosZDouble(10))

    "LocalDate" should {
      case class DateKey(date: LocalDate)
      object DateKey {
        import fdb_key._
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
        import fdb_key._
        import FdbKeyCodecTest.defaultZoneIdLocalDateTimeKeySerdes
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

    "Double" should {
      case class DoubleKey(value: Double)
      object DoubleKey {
        import fdb_key._
        implicit val dbKey = KeySerdes[DoubleKey]
      }

      "compare < 0" in {
        forAll { (a: Double, b: Double) =>
          whenever(a < b) {
            assert(KeySerdes.compare(KeySerdes.serialize(DoubleKey(a)), KeySerdes.serialize(DoubleKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: Double =>
          assert(KeySerdes.compare(KeySerdes.serialize(DoubleKey(a)), KeySerdes.serialize(DoubleKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: Double, b: Double) =>
          whenever(a > b) {
            assert(KeySerdes.compare(KeySerdes.serialize(DoubleKey(a)), KeySerdes.serialize(DoubleKey(b))) > 0)
          }
        }
      }
    }

    "LocalTime" should {
      case class LocalTimeKey(value: LocalTime)
      object LocalTimeKey {
        import fdb_key._
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
        import fdb_key._
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
        import fdb_key._
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
        import fdb_key._
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
        import fdb_key._
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
        import fdb_key._
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
        import fdb_key._
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

      "ByteEnum" should {
        "serialize / deserialize" in {
          forAll { (entry: ByteEnumTest) =>
            import fdb_key._
            KeySerdes.deserialize[ByteEnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }

      "IntEnum" should {
        "serialize / deserialize" in {
          forAll { (entry: IntEnumTest) =>
            import fdb_key._
            KeySerdes.deserialize[IntEnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }

      "Enum" should {
        "serialize / deserialize" in {
          forAll { (entry: EnumTest) =>
            import fdb_key._
            KeySerdes.deserialize[EnumTest](KeySerdes.serialize(entry)) should equal(Right(entry))
          }
        }
      }
    }

    "refined" should {
      "serdes" in {
        import eu.timepit.refined.auto._
        import fdb_key._
        val key = TestKeyWithRefined("foo", 1234)
        KeySerdes.deserialize[TestKeyWithRefined](KeySerdes.serialize(key)) should equal(Right(key))
      }

      "serdes alias" in {
        import eu.timepit.refined.auto._
        import fdb_key._
        val foo: NonEmptyString = "foo"
        val key = TestKeyWithRefinedAlias(1234, foo)
        import eu.timepit.refined.shapeless.typeable._
        implicit val dbKey = KeySerdes[TestKeyWithRefinedAlias]
        KeySerdes.deserialize[TestKeyWithRefinedAlias](KeySerdes.serialize(key)) should equal(Right(key))
      }
    }

    /*"sealed trait" should {
      "serdes" in {
//        import fdb_key._
        val foo = KeyWithSealedTraitTest(SealedTraitTestFoo(1))
        val bar = KeyWithSealedTraitTest(SealedTraitTestBar(2))
        val baz = KeyWithSealedTraitTest(SealedTraitTestBaz(3))
        val boo = KeyWithSealedTraitTest(SealedTraitTestBoo(4))

        KeySerdes.deserialize[KeyWithSealedTraitTest](KeySerdes.serialize[KeyWithSealedTraitTest](foo)) should equal(
          Right(foo)
        )
        KeySerdes.deserialize[KeyWithSealedTraitTest](KeySerdes.serialize[KeyWithSealedTraitTest](bar)) should equal(
          Right(bar)
        )
        KeySerdes.deserialize[KeyWithSealedTraitTest](KeySerdes.serialize[KeyWithSealedTraitTest](baz)) should equal(
          Right(baz)
        )
        KeySerdes.deserialize[KeyWithSealedTraitTest](KeySerdes.serialize[KeyWithSealedTraitTest](boo)) should equal(
          Right(boo)
        )
      }
    }*/

    "KeyPrefixEvidence" should {
      "be contravariant" in {
        KeyPrefix[(EnumTest.One.type, IntEnumTest.Two.type), ContravariantKeyPrefixTest] should be(
          ContravariantKeyPrefixTest.dbKeyPrefix
        )
      }
    }
  }
}
