package dev.chopsticks.avro4s.test

import com.sksamuel.avro4s.{AvroAlias, AvroName, SchemaFor}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object AvroEnumTest {
  @AvroAlias("Meh")
  @AvroName("Boo")
  sealed trait Base
  case object Foo extends Base
  case object Bar extends Base
}

final class AvroEnumTest extends AnyWordSpecLike with Assertions with Matchers {
  import AvroEnumTest._
  "enum with AvroAlias annotation" in {
    SchemaFor[Base].schema.toString(true) should equal(
      """{
        |  "type" : "enum",
        |  "name" : "Boo",
        |  "namespace" : "dev.chopsticks.avro4s.test.AvroEnumTest",
        |  "symbols" : [ "Bar", "Foo" ],
        |  "aliases" : [ "Meh" ]
        |}""".stripMargin
    )
  }
}
