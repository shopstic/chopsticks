package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import dev.chopsticks.testkit.ArbitraryTime._
import org.scalactic.anyvals.{PosInt, PosZDouble}
import org.scalatest.{Assertions, Matchers, WordSpecLike}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import dev.chopsticks.kvdb.codec.berkeleydb_key._

//noinspection TypeAnnotation
class DbKeyTest extends WordSpecLike with Assertions with Matchers with ScalaCheckDrivenPropertyChecks {
  "Scala tuple" should {
    type TupleKey = (Boolean, Option[Int], String)
    implicit val tupleDbKey = DbKey[TupleKey]

    type NestedKey = (Int, ((Boolean, (Long, LocalDate), String), Double))
    implicit val nestedDbKey = DbKey[NestedKey]

    type FlattenedKey = (Int, Boolean, Long, LocalDate, String, Double)
    implicit val flattenedDbKey = DbKey[FlattenedKey]

    "encode and decode correctly" in {
      val key: TupleKey = (true, Some(123), "cool")
      assert(DbKey.decode[TupleKey](DbKey.encode(key)) == Right(key))
    }

    "treat nested tuple the same with flattened version" in {
      val nested: FlattenedKey = (123, true, 500L, LocalDate.of(2018, 1, 2), "foo bar", 99.887d)
      val flattened: NestedKey = (123, ((true, (500L, LocalDate.of(2018, 1, 2)), "foo bar"), 99.887d))

      assert(DbKey.isEqual(DbKey.encode(nested), DbKey.encode(flattened)))
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
      implicit val dbKey = DbKey[Person]
    }

    "encode and decode correctly" in {
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
      assert(DbKey.decode[Person](DbKey.encode(person1)) == Right(person1))

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
      assert(DbKey.decode[Person](DbKey.encode(person2)) == Right(person2))
    }
  }

  "prefix" should {
    case class TradeTick(symbol: String, dateTime: LocalDateTime, price: BigDecimal, size: Int)
    object TradeTick {
      implicit val dbKey = DbKey[TradeTick]
    }

    case class Prefix(symbol: String, dateTime: LocalDateTime)
    object Prefix {
      implicit val dbKeyPrefix = DbKeyPrefix[Prefix, TradeTick]
    }

    "encode" in {
      val now = LocalDateTime.now

      val tick = TradeTick("AAPL", now, BigDecimal("1234.67"), 1000)
      val prefix = Prefix("AAPL", now)
      assert(DbKey.isPrefix(DbKeyPrefix[Prefix, TradeTick].encode(prefix), DbKey.encode(tick)))
    }
  }

//  "legacy as-it string keys" should {
//    "compile for DbKeyPrefix" in {
//      implicitly[DbKeyPrefix[String, String]]
//    }
//
//    "compile for DbKey" in {
//      implicitly[DbKey[String]]
//    }
//
//    "not implicitly compile for Product, we want to enforce it explicitly" in {
//      assertDoesNotCompile("""
//          |implicitly[DbKey[Tuple1[String]]]
//        """.stripMargin)
//    }
//  }

  "compare" when {
    implicit val generatorDrivenConfig =
      PropertyCheckConfiguration(minSuccessful = PosInt(100), maxDiscardedFactor = PosZDouble(10))

    "LocalDate" should {
      case class DateKey(date: LocalDate)
      object DateKey {
        implicit val dbKey = DbKey[DateKey]
      }

      "compare < 0" in {
        forAll { (a: LocalDate, b: LocalDate) =>
          whenever(a.isBefore(b)) {
            assert(DbKey.compare(DbKey.encode(DateKey(a)), DbKey.encode(DateKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalDate =>
          assert(DbKey.compare(DbKey.encode(DateKey(a)), DbKey.encode(DateKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalDate, b: LocalDate) =>
          whenever(a.isAfter(b)) {
            assert(DbKey.compare(DbKey.encode(DateKey(a)), DbKey.encode(DateKey(b))) > 0)
          }
        }
      }
    }

    "LocalDateTime" should {
      case class DateTimeKey(date: LocalDateTime)
      object DateTimeKey {
        implicit val dbKey = DbKey[DateTimeKey]
      }

      "compare < 0" in {
        forAll { (a: LocalDateTime, b: LocalDateTime) =>
          whenever(a.isBefore(b)) {
            assert(DbKey.compare(DbKey.encode(DateTimeKey(a)), DbKey.encode(DateTimeKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalDateTime =>
          assert(DbKey.compare(DbKey.encode(DateTimeKey(a)), DbKey.encode(DateTimeKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalDateTime, b: LocalDateTime) =>
          whenever(a.isAfter(b)) {
            assert(DbKey.compare(DbKey.encode(DateTimeKey(a)), DbKey.encode(DateTimeKey(b))) > 0)
          }
        }
      }
    }

    "BigDecimal" should {
      case class BigDecimalKey(value: BigDecimal)
      object BigDecimalKey {
        implicit val dbKey = DbKey[BigDecimalKey]
      }

      "compare < 0" in {
        forAll { (a: BigDecimal, b: BigDecimal) =>
          whenever(a < b) {
            assert(DbKey.compare(DbKey.encode(BigDecimalKey(a)), DbKey.encode(BigDecimalKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: BigDecimal =>
          assert(DbKey.compare(DbKey.encode(BigDecimalKey(a)), DbKey.encode(BigDecimalKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: BigDecimal, b: BigDecimal) =>
          whenever(a > b) {
            assert(DbKey.compare(DbKey.encode(BigDecimalKey(a)), DbKey.encode(BigDecimalKey(b))) > 0)
          }
        }
      }
    }

    "LocalTime" should {
      case class LocalTimeKey(value: LocalTime)
      object LocalTimeKey {
        implicit val dbKey = DbKey[LocalTimeKey]
      }

      "compare < 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isBefore(b)) {
            assert(DbKey.compare(DbKey.encode(LocalTimeKey(a)), DbKey.encode(LocalTimeKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: LocalTime =>
          assert(DbKey.compare(DbKey.encode(LocalTimeKey(a)), DbKey.encode(LocalTimeKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isAfter(b)) {
            assert(DbKey.compare(DbKey.encode(LocalTimeKey(a)), DbKey.encode(LocalTimeKey(b))) > 0)
          }
        }
      }
    }

    "Option" should {
      case class LocalTimeKey(value: Option[LocalTime])
      object LocalTimeKey {
        implicit val dbKey = DbKey[LocalTimeKey]
      }

      "compare < 0" in {
        forAll { b: LocalTime =>
          assert(DbKey.compare(DbKey.encode(LocalTimeKey(None)), DbKey.encode(LocalTimeKey(Some(b)))) < 0)
        }
      }

      "compare > 0" in {
        forAll { (a: LocalTime, b: LocalTime) =>
          whenever(a.isAfter(b)) {
            assert(DbKey.compare(DbKey.encode(LocalTimeKey(Some(a))), DbKey.encode(LocalTimeKey(Some(b)))) > 0)
          }
        }
      }
    }

    "YearMonth" should {
      case class YearMonthKey(value: YearMonth)
      object YearMonthKey {
        implicit val dbKey = DbKey[YearMonthKey]
      }

      "compare < 0" in {
        forAll { (a: YearMonth, b: YearMonth) =>
          whenever(a.isBefore(b)) {
            assert(DbKey.compare(DbKey.encode(YearMonthKey(a)), DbKey.encode(YearMonthKey(b))) < 0)
          }
        }
      }

      "compare == 0" in {
        forAll { a: YearMonth =>
          assert(DbKey.compare(DbKey.encode(YearMonthKey(a)), DbKey.encode(YearMonthKey(a))) == 0)
        }
      }

      "compare > 0" in {
        forAll { (a: YearMonth, b: YearMonth) =>
          whenever(a.isAfter(b)) {
            assert(DbKey.compare(DbKey.encode(YearMonthKey(a)), DbKey.encode(YearMonthKey(b))) > 0)
          }
        }
      }
    }

    "Int" should {
      case class IntKey(value: Int)
      object IntKey {
        implicit val dbKey = DbKey[IntKey]
      }

      "compare < 0" in {
        forAll { (a: Int, b: Int) =>
          whenever(a < b) {
            assert(DbKey.compare(DbKey.encode(IntKey(a)), DbKey.encode(IntKey(b))) < 0)
          }
        }
      }

      "compare > 0" in {
        forAll { (a: Int, b: Int) =>
          whenever(a > b) {
            assert(DbKey.compare(DbKey.encode(IntKey(a)), DbKey.encode(IntKey(b))) > 0)
          }
        }
      }
    }

    "Long" should {
      case class LongKey(value: Long)
      object LongKey {
        implicit val dbKey = DbKey[LongKey]
      }

      "compare < 0" in {
        forAll { (a: Long, b: Long) =>
          whenever(a < b) {
            assert(DbKey.compare(DbKey.encode(LongKey(a)), DbKey.encode(LongKey(b))) < 0)
          }
        }
      }

      "compare > 0" in {
        forAll { (a: Long, b: Long) =>
          whenever(a > b) {
            assert(DbKey.compare(DbKey.encode(LongKey(a)), DbKey.encode(LongKey(b))) > 0)
          }
        }
      }
    }

    "Boolean" should {
      case class BooleanKey(value: Boolean)
      object BooleanKey {
        implicit val dbKey = DbKey[BooleanKey]
      }

      "compare" in {
        assert(DbKey.compare(DbKey.encode(BooleanKey(false)), DbKey.encode(BooleanKey(true))) < 0)
        assert(DbKey.compare(DbKey.encode(BooleanKey(true)), DbKey.encode(BooleanKey(true))) == 0)
        assert(DbKey.compare(DbKey.encode(BooleanKey(false)), DbKey.encode(BooleanKey(false))) == 0)
      }
    }

    "UUID" should {
      case class UuidKey(value: UUID, foo: Boolean)
      object UuidKey {
        implicit val dbKey = DbKey[UuidKey]
      }
      "encode / decode" in {
        forAll { (uuid: UUID, foo: Boolean) =>
          val key = UuidKey(uuid, foo)
          DbKey.decode[UuidKey](DbKey.encode(key)) should equal(Right(key))
        }
      }
    }
  }
}
