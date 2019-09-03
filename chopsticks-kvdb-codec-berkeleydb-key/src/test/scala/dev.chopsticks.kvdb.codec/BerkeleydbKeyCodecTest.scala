package dev.chopsticks.kvdb.codec

import java.time.{LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import org.scalatest.{Assertions, Matchers, WordSpecLike}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import dev.chopsticks.testkit.ArbitraryTime._
import org.scalactic.anyvals.{PosInt, PosZDouble}

//noinspection TypeAnnotation
class BerkeleydbKeyCodecTest extends WordSpecLike with Assertions with Matchers with ScalaCheckDrivenPropertyChecks {
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
    case class TradeTick(symbol: String, dateTime: LocalDateTime, price: BigDecimal, size: Int)
    object TradeTick {
      import berkeleydb_key._
      implicit val dbKey = KeySerdes[TradeTick]
    }

    case class Prefix(symbol: String, dateTime: LocalDateTime)
    object Prefix {
      import berkeleydb_key._
      implicit val dbKeyPrefix = KeyPrefix[Prefix, TradeTick]
    }

    "serialize" in {
      val now = LocalDateTime.now

      val tick = TradeTick("AAPL", now, BigDecimal("1234.67"), 1000)
      val prefix = Prefix("AAPL", now)
      assert(KeySerdes.isPrefix(KeyPrefix[Prefix, TradeTick].serialize(prefix), KeySerdes.serialize(tick)))
    }
  }

  "legacy literal string keys" should {
    import dev.chopsticks.kvdb.codec.primitive._
    "compile for KeyPrefix" in {
      implicitly[KeyPrefix[String, String]]
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

  "different codecs between the key and its prefix" should {
    final case class TestKey(foo: String)
    "not compile" in {
      assertDoesNotCompile("""
                             |implicit val oneDbKey = {
                             |  import dev.chopsticks.kvdb.codec.berkeleydb_key._
                             |  KeySerdes[TestKey]
                             |}
                             |import dev.chopsticks.kvdb.codec.primitive._
                             |implicitly[KeyPrefix[String, TestKey]]
        """.stripMargin)
    }
  }

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
  }
}
