package dev.chopsticks.util.config

import com.typesafe.config.ConfigFactory
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import pureconfig.ConfigConvert
import eu.timepit.refined.auto._

object PureconfigConvertersTest {
  final case class StringValueClass(foo: String) extends AnyVal
  final case class NonEmptyStringCaseClass(foo: NonEmptyString)
  final case class MoreThanOneFieldCaseClass(foo: NonEmptyString, bar: Boolean)
}

final class PureconfigConvertersTest extends AnyWordSpecLike with Assertions with Matchers {
  import PureconfigConvertersTest._

  "deriveFlat" should {
    "work with a value class having a single field" in {
      val converter: ConfigConvert[StringValueClass] = PureconfigConverters.deriveFlat

      val original = StringValueClass("foo")
      val expected = ConfigFactory.parseString("test = foo").getValue("test")

      converter.to(original) should be(expected)
      converter.from(expected) should be(Right(original))
    }

    "work with a case class having a single refined field" in {
      val converter: ConfigConvert[NonEmptyStringCaseClass] = {
        import PureconfigConverters._
        deriveFlat
      }

      val original = NonEmptyStringCaseClass("foo")
      val expected = ConfigFactory.parseString("test = foo").getValue("test")

      converter.to(original) should be(expected)
      converter.from(expected) should be(Right(original))
    }

    "not compile if the case class does not have exactly 1 field" in {
      assertDoesNotCompile("""
          |val converter: ConfigConvert[MoreThanOneFieldCaseClass] = {
          |  import PureconfigConverters._
          |  deriveFlatConverter
          |}
          |""".stripMargin)
    }
  }
}
