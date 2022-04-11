package dev.chopsticks.avro4s.test

import com.sksamuel.avro4s.{
  AvroDoc,
  AvroName,
  AvroNamespace,
  AvroOneOf,
  AvroOneOfUnknownSubtype,
  Decoder,
  Encoder,
  InvalidAvroOneOfDefaultValue,
  SchemaFor
}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

sealed trait UnionBase
object UnionBase {
  final case class Foo(foo: String) extends UnionBase
  final case class Bar(bar: Int) extends UnionBase
}

@AvroOneOf(OneOfBase.Unknown())
@AvroDoc("Some documentation goes here")
sealed trait OneOfBase
object OneOfBase {
  final case class Unknown(subtype: Option[String] = None) extends OneOfBase with AvroOneOfUnknownSubtype {
    override def withSubtype(subtype: String): Unknown.this.type = copy(Some(subtype))
  }
  final case class Foo(foo: String) extends OneOfBase
  final case class Bar(bar: Int) extends OneOfBase
}

@AvroName("EvolvableBase")
@AvroOneOf(EvolvedOneOfBase.Unknown)
sealed trait EvolvedOneOfBase
object EvolvedOneOfBase {
  @AvroNamespace("dev.chopsticks.avro4s.test.EvolvableBase")
  case object Unknown extends EvolvedOneOfBase

  @AvroNamespace("dev.chopsticks.avro4s.test.EvolvableBase")
  final case class Foo(foo: String) extends EvolvedOneOfBase
}

@AvroOneOf(NestingOneOfBase.Unknown)
sealed trait NestingOneOfBase
object NestingOneOfBase {
  case object Unknown extends NestingOneOfBase
  final case class Nested(foo: OneOfBase) extends NestingOneOfBase
  final case class SomethingElse(bar: Int) extends NestingOneOfBase
}

@AvroOneOf(EvolvedOneOfBase.Unknown) // Intentionally pass an invalid default value
sealed trait BadDefaultValueInAnnotation
object BadDefaultValueInAnnotation {
  final case class Foo() extends BadDefaultValueInAnnotation
  case object Bar extends BadDefaultValueInAnnotation
}

final class AvroOneOfTest extends AnyWordSpecLike with Assertions with Matchers {
  "sealed traits without AvroOneOf annotation" should {
    "derive as native Avro UNIONs" in {
      SchemaFor[UnionBase].schema.toString(true) should equal("""[ {
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

      val foo = UnionBase.Foo("foo")
      val bar = UnionBase.Bar(123)

      Decoder[UnionBase].decode(Encoder[UnionBase].encode(foo)) should equal(foo)
      Decoder[UnionBase].decode(Encoder[UnionBase].encode(bar)) should equal(bar)
    }
  }

  "sealed traits with AvroOneOf annotation" should {
    "work with encoder and decoder withSchema method" in {
      Encoder[OneOfBase].withSchema(SchemaFor[OneOfBase])
      Decoder[OneOfBase].withSchema(SchemaFor[OneOfBase])
    }

    "derive a different schema with a custom Encoder and Decoder that support schema evolution" in {
      SchemaFor[OneOfBase].schema.toString(true) should equal("""{
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

      val foo = OneOfBase.Foo("foo")
      val bar = OneOfBase.Bar(123)

      Decoder[OneOfBase].decode(Encoder[OneOfBase].encode(foo)) should equal(foo)
      Decoder[OneOfBase].decode(Encoder[OneOfBase].encode(bar)) should equal(bar)
    }

    "derive recursively with nested evolvable unions" in {
      SchemaFor[NestingOneOfBase].schema.toString(true) should equal("""{
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

      val foo = NestingOneOfBase.Nested(OneOfBase.Foo("foo"))
      val bar = NestingOneOfBase.SomethingElse(123)

      Decoder[NestingOneOfBase].decode(Encoder[NestingOneOfBase].encode(foo)) should equal(foo)
      Decoder[NestingOneOfBase].decode(Encoder[NestingOneOfBase].encode(bar)) should equal(bar)
    }

    "decode to the specified default value if the corresponding field is not found" in {
      Decoder[EvolvedOneOfBase].decode(Encoder[OneOfBase].encode(OneOfBase.Foo("foo"))) should equal(
        EvolvedOneOfBase.Foo("foo")
      )
      Decoder[EvolvedOneOfBase].decode(Encoder[OneOfBase].encode(OneOfBase.Bar(123))) should equal(
        EvolvedOneOfBase.Unknown
      )
    }

    "throw an exception if the supplied default value is not one of the valid subtypes" in {
      val decoder = Decoder[BadDefaultValueInAnnotation]
      val encoder = Encoder[BadDefaultValueInAnnotation]

      assertThrows[InvalidAvroOneOfDefaultValue] {
        decoder.decode(encoder.encode(BadDefaultValueInAnnotation.Foo()))
      }
    }
  }
}
