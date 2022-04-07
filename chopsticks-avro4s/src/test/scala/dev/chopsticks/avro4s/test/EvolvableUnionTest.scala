package dev.chopsticks.avro4s.test

import com.sksamuel.avro4s.{
  AvroDoc,
  AvroEvolvableUnion,
  AvroName,
  AvroNamespace,
  Decoder,
  Encoder,
  InvalidAvroEvolvableUnionDefaultValue,
  SchemaFor
}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

sealed trait ClassicBase
object ClassicBase {
  final case class Foo(foo: String) extends ClassicBase
  final case class Bar(bar: Int) extends ClassicBase
}

@AvroEvolvableUnion(EvolvableBase.Unknown)
@AvroDoc("Some documentation goes here")
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

@AvroEvolvableUnion(NestingEvolvableBase.Unknown)
sealed trait NestingEvolvableBase
object NestingEvolvableBase {
  case object Unknown extends NestingEvolvableBase
  final case class Nested(foo: EvolvableBase) extends NestingEvolvableBase
  final case class SomethingElse(bar: Int) extends NestingEvolvableBase
}

@AvroEvolvableUnion(EvolvedBase.Unknown) // Intentionally pass an invalid default value
sealed trait BadDefaultValueInAnnotation
object BadDefaultValueInAnnotation {
  final case class Foo() extends BadDefaultValueInAnnotation
  case object Bar extends BadDefaultValueInAnnotation
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
        |  "doc" : "Some documentation goes here",
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

    "derive recursively with nested evolvable unions" in {
      SchemaFor[NestingEvolvableBase].schema.toString(true) should equal("""{
         |  "type" : "record",
         |  "name" : "NestingEvolvableBase",
         |  "namespace" : "dev.chopsticks.avro4s.test",
         |  "fields" : [ {
         |    "name" : "kind",
         |    "type" : "string",
         |    "doc" : ""
         |  }, {
         |    "name" : "coproducts",
         |    "type" : {
         |      "type" : "record",
         |      "name" : "NestingEvolvableBase_Coproducts",
         |      "doc" : "",
         |      "fields" : [ {
         |        "name" : "NestingEvolvableBase_Nested",
         |        "type" : [ {
         |          "type" : "record",
         |          "name" : "Nested",
         |          "namespace" : "dev.chopsticks.avro4s.test.NestingEvolvableBase",
         |          "fields" : [ {
         |            "name" : "foo",
         |            "type" : {
         |              "type" : "record",
         |              "name" : "EvolvableBase",
         |              "namespace" : "dev.chopsticks.avro4s.test",
         |              "doc" : "Some documentation goes here",
         |              "fields" : [ {
         |                "name" : "kind",
         |                "type" : "string",
         |                "doc" : ""
         |              }, {
         |                "name" : "coproducts",
         |                "type" : {
         |                  "type" : "record",
         |                  "name" : "EvolvableBase_Coproducts",
         |                  "doc" : "",
         |                  "fields" : [ {
         |                    "name" : "EvolvableBase_Bar",
         |                    "type" : [ {
         |                      "type" : "record",
         |                      "name" : "Bar",
         |                      "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
         |                      "fields" : [ {
         |                        "name" : "bar",
         |                        "type" : "int"
         |                      } ]
         |                    }, "null" ],
         |                    "doc" : ""
         |                  }, {
         |                    "name" : "EvolvableBase_Foo",
         |                    "type" : [ {
         |                      "type" : "record",
         |                      "name" : "Foo",
         |                      "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
         |                      "fields" : [ {
         |                        "name" : "foo",
         |                        "type" : "string"
         |                      } ]
         |                    }, "null" ],
         |                    "doc" : ""
         |                  }, {
         |                    "name" : "EvolvableBase_Unknown",
         |                    "type" : [ {
         |                      "type" : "record",
         |                      "name" : "Unknown",
         |                      "namespace" : "dev.chopsticks.avro4s.test.EvolvableBase",
         |                      "fields" : [ ]
         |                    }, "null" ],
         |                    "doc" : ""
         |                  } ]
         |                },
         |                "doc" : ""
         |              } ]
         |            }
         |          } ]
         |        }, "null" ],
         |        "doc" : ""
         |      }, {
         |        "name" : "NestingEvolvableBase_SomethingElse",
         |        "type" : [ {
         |          "type" : "record",
         |          "name" : "SomethingElse",
         |          "namespace" : "dev.chopsticks.avro4s.test.NestingEvolvableBase",
         |          "fields" : [ {
         |            "name" : "bar",
         |            "type" : "int"
         |          } ]
         |        }, "null" ],
         |        "doc" : ""
         |      }, {
         |        "name" : "NestingEvolvableBase_Unknown",
         |        "type" : [ {
         |          "type" : "record",
         |          "name" : "Unknown",
         |          "namespace" : "dev.chopsticks.avro4s.test.NestingEvolvableBase",
         |          "fields" : [ ]
         |        }, "null" ],
         |        "doc" : ""
         |      } ]
         |    },
         |    "doc" : ""
         |  } ]
         |}""".stripMargin)

      val foo = NestingEvolvableBase.Nested(EvolvableBase.Foo("foo"))
      val bar = NestingEvolvableBase.SomethingElse(123)

      Decoder[NestingEvolvableBase].decode(Encoder[NestingEvolvableBase].encode(foo)) should equal(foo)
      Decoder[NestingEvolvableBase].decode(Encoder[NestingEvolvableBase].encode(bar)) should equal(bar)
    }

    "decode to the specified default value if the corresponding field is not found" in {
      Decoder[EvolvedBase].decode(Encoder[EvolvableBase].encode(EvolvableBase.Foo("foo"))) should equal(
        EvolvedBase.Foo("foo")
      )
      Decoder[EvolvedBase].decode(Encoder[EvolvableBase].encode(EvolvableBase.Bar(123))) should equal(
        EvolvedBase.Unknown
      )
    }

    "throw an exception if the supplied default value is not one of the valid subtypes" in {
      val decoder = Decoder[BadDefaultValueInAnnotation]
      val encoder = Encoder[BadDefaultValueInAnnotation]

      assertThrows[InvalidAvroEvolvableUnionDefaultValue] {
        decoder.decode(encoder.encode(BadDefaultValueInAnnotation.Foo()))
      }
    }
  }
}
