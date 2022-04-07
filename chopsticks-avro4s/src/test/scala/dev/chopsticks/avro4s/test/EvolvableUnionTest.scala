package dev.chopsticks.avro4s.test

import com.sksamuel.avro4s.{AvroEvolvableUnion, AvroName, AvroNamespace, Decoder, Encoder, SchemaFor}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

sealed trait ClassicBase
object ClassicBase {
  final case class Foo(foo: String) extends ClassicBase
  final case class Bar(bar: Int) extends ClassicBase
}

@AvroEvolvableUnion(EvolvableBase.Unknown)
sealed trait EvolvableBase
object EvolvableBase {
  case object Unknown extends EvolvableBase
  final case class Foo(foo: String) extends EvolvableBase
  final case class Bar(bar: Int) extends EvolvableBase
}

@AvroName("EvolvableBase")
@AvroEvolvableUnion(EvolvedBase.Unknown)
sealed trait EvolvedBase
object EvolvedBase {
  @AvroNamespace("dev.chopsticks.avro4s.test.EvolvableBase")
  case object Unknown extends EvolvedBase

  @AvroNamespace("dev.chopsticks.avro4s.test.EvolvableBase")
  final case class Foo(foo: String) extends EvolvedBase
}

final class EvolvableUnionTest extends AnyWordSpecLike with Assertions with Matchers {
  "sealed traits without AvroEvolvableUnion annotation" should {
    "derive as native Avro UNIONs" in {
      SchemaFor[ClassicBase].schema.toString(true) should equal("""[ {
          |  "type" : "record",
          |  "name" : "Bar",
          |  "namespace" : "dev.chopsticks.avro4s.test.ClassicBase",
          |  "fields" : [ {
          |    "name" : "bar",
          |    "type" : "int"
          |  } ]
          |}, {
          |  "type" : "record",
          |  "name" : "Foo",
          |  "namespace" : "dev.chopsticks.avro4s.test.ClassicBase",
          |  "fields" : [ {
          |    "name" : "foo",
          |    "type" : "string"
          |  } ]
          |} ]""".stripMargin)

      val foo = ClassicBase.Foo("foo")
      val bar = ClassicBase.Bar(123)

      Decoder[ClassicBase].decode(Encoder[ClassicBase].encode(foo)) should equal(foo)
      Decoder[ClassicBase].decode(Encoder[ClassicBase].encode(bar)) should equal(bar)
    }
  }

  "sealed traits with AvroEvolvableUnion annotation" should {
    "derive a different schema with a custom Encoder and Decoder that support schema evolution" in {
      SchemaFor[EvolvableBase].schema.toString(true) should equal("""{
       |  "type" : "record",
       |  "name" : "EvolvableBase",
       |  "namespace" : "dev.chopsticks.avro4s.test",
       |  "doc" : "",
       |  "fields" : [ {
       |    "name" : "kind",
       |    "type" : "string",
       |    "doc" : ""
       |  }, {
       |    "name" : "coproducts",
       |    "type" : {
       |      "type" : "record",
       |      "name" : "EvolvableBase_Coproducts",
       |      "doc" : "",
       |      "fields" : [ {
       |        "name" : "EvolvableBase_Bar",
       |        "type" : [ {
       |          "type" : "record",
       |          "name" : "Bar",
       |          "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
       |          "fields" : [ {
       |            "name" : "bar",
       |            "type" : "int"
       |          } ]
       |        }, "null" ],
       |        "doc" : ""
       |      }, {
       |        "name" : "EvolvableBase_Foo",
       |        "type" : [ {
       |          "type" : "record",
       |          "name" : "Foo",
       |          "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
       |          "fields" : [ {
       |            "name" : "foo",
       |            "type" : "string"
       |          } ]
       |        }, "null" ],
       |        "doc" : ""
       |      }, {
       |        "name" : "EvolvableBase_Unknown",
       |        "type" : [ {
       |          "type" : "record",
       |          "name" : "Unknown",
       |          "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
       |          "fields" : [ ]
       |        }, "null" ],
       |        "doc" : ""
       |      } ]
       |    },
       |    "doc" : ""
       |  } ]
       |}""".stripMargin)

      val foo = EvolvableBase.Foo("foo")
      val bar = EvolvableBase.Bar(123)

      Decoder[EvolvableBase].decode(Encoder[EvolvableBase].encode(foo)) should equal(foo)
      Decoder[EvolvableBase].decode(Encoder[EvolvableBase].encode(bar)) should equal(bar)
    }

    "Decode to the specified default value if the corresponding field is not found" in {
      Decoder[EvolvedBase].decode(Encoder[EvolvableBase].encode(EvolvableBase.Foo("foo"))) should equal(EvolvedBase.Foo("foo"))
      Decoder[EvolvedBase].decode(Encoder[EvolvableBase].encode(EvolvableBase.Bar(123))) should equal(EvolvedBase.Unknown)
    }
  }
}
