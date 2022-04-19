package dev.chopsticks.avro4s.test

import com.sksamuel.avro4s.{
  AvroDoc,
  AvroIgnoreSubtype,
  AvroName,
  AvroNamespace,
  AvroOneOf,
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

@AvroOneOf(OneOfBase.Unknown)
@AvroDoc("Some documentation goes here")
sealed trait OneOfBase
object OneOfBase {
  @AvroIgnoreSubtype
  case object Unknown extends OneOfBase
  final case class Foo(foo: String) extends OneOfBase
  final case class Bar(bar: Int) extends OneOfBase
}

@AvroName("EvolvableBase")
@AvroOneOf(EvolvedOneOfBase.Unknown)
sealed trait EvolvedOneOfBase
object EvolvedOneOfBase {
  @AvroNamespace("dev.chopsticks.avro4s.test.OneOfBase")
  @AvroIgnoreSubtype
  case object Unknown extends EvolvedOneOfBase

  @AvroNamespace("dev.chopsticks.avro4s.test.OneOfBase")
  final case class Foo(foo: String) extends EvolvedOneOfBase

  @AvroNamespace("dev.chopsticks.avro4s.test.OneOfBase")
  final case class Baz(baz: Double) extends EvolvedOneOfBase
}

@AvroOneOf(NestingOneOfBase.Unknown)
sealed trait NestingOneOfBase
object NestingOneOfBase {
  @AvroIgnoreSubtype
  case object Unknown extends NestingOneOfBase
  final case class Nested(foo: OneOfBase) extends NestingOneOfBase
  final case class Recursive(foo: NestingOneOfBase) extends NestingOneOfBase
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
          |  "namespace" : "dev.chopsticks.avro4s.test.UnionBase",
          |  "fields" : [ {
          |    "name" : "bar",
          |    "type" : "int"
          |  } ]
          |}, {
          |  "type" : "record",
          |  "name" : "Foo",
          |  "namespace" : "dev.chopsticks.avro4s.test.UnionBase",
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
        |  "name" : "OneOfBase",
        |  "namespace" : "dev.chopsticks.avro4s.test",
        |  "doc" : "Some documentation goes here",
        |  "fields" : [ {
        |    "name" : "OneOfBase_Bar",
        |    "type" : [ "null", {
        |      "type" : "record",
        |      "name" : "Bar",
        |      "namespace" : "dev.chopsticks.avro4s.test.OneOfBase",
        |      "fields" : [ {
        |        "name" : "bar",
        |        "type" : "int"
        |      } ]
        |    } ],
        |    "doc" : "",
        |    "default" : null
        |  }, {
        |    "name" : "OneOfBase_Foo",
        |    "type" : [ "null", {
        |      "type" : "record",
        |      "name" : "Foo",
        |      "namespace" : "dev.chopsticks.avro4s.test.OneOfBase",
        |      "fields" : [ {
        |        "name" : "foo",
        |        "type" : "string"
        |      } ]
        |    } ],
        |    "doc" : "",
        |    "default" : null
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
         |  "name" : "NestingOneOfBase",
         |  "namespace" : "dev.chopsticks.avro4s.test",
         |  "fields" : [ {
         |    "name" : "NestingOneOfBase_Nested",
         |    "type" : [ "null", {
         |      "type" : "record",
         |      "name" : "Nested",
         |      "namespace" : "dev.chopsticks.avro4s.test.NestingOneOfBase",
         |      "fields" : [ {
         |        "name" : "foo",
         |        "type" : {
         |          "type" : "record",
         |          "name" : "OneOfBase",
         |          "namespace" : "dev.chopsticks.avro4s.test",
         |          "doc" : "Some documentation goes here",
         |          "fields" : [ {
         |            "name" : "OneOfBase_Bar",
         |            "type" : [ "null", {
         |              "type" : "record",
         |              "name" : "Bar",
         |              "namespace" : "dev.chopsticks.avro4s.test.OneOfBase",
         |              "fields" : [ {
         |                "name" : "bar",
         |                "type" : "int"
         |              } ]
         |            } ],
         |            "doc" : "",
         |            "default" : null
         |          }, {
         |            "name" : "OneOfBase_Foo",
         |            "type" : [ "null", {
         |              "type" : "record",
         |              "name" : "Foo",
         |              "namespace" : "dev.chopsticks.avro4s.test.OneOfBase",
         |              "fields" : [ {
         |                "name" : "foo",
         |                "type" : "string"
         |              } ]
         |            } ],
         |            "doc" : "",
         |            "default" : null
         |          } ]
         |        }
         |      } ]
         |    } ],
         |    "doc" : "",
         |    "default" : null
         |  }, {
         |    "name" : "NestingOneOfBase_Recursive",
         |    "type" : [ "null", {
         |      "type" : "record",
         |      "name" : "Recursive",
         |      "namespace" : "dev.chopsticks.avro4s.test.NestingOneOfBase",
         |      "fields" : [ {
         |        "name" : "foo",
         |        "type" : "dev.chopsticks.avro4s.test.NestingOneOfBase"
         |      } ]
         |    } ],
         |    "doc" : "",
         |    "default" : null
         |  }, {
         |    "name" : "NestingOneOfBase_SomethingElse",
         |    "type" : [ "null", {
         |      "type" : "record",
         |      "name" : "SomethingElse",
         |      "namespace" : "dev.chopsticks.avro4s.test.NestingOneOfBase",
         |      "fields" : [ {
         |        "name" : "bar",
         |        "type" : "int"
         |      } ]
         |    } ],
         |    "doc" : "",
         |    "default" : null
         |  } ]
         |}""".stripMargin)

      val nested = NestingOneOfBase.Nested(OneOfBase.Foo("foo"))
      val recursive = NestingOneOfBase.Recursive(nested)
      val somethingElse = NestingOneOfBase.SomethingElse(123)

      Decoder[NestingOneOfBase].decode(Encoder[NestingOneOfBase].encode(nested)) should equal(nested)
      Decoder[NestingOneOfBase].decode(Encoder[NestingOneOfBase].encode(recursive)) should equal(recursive)
      Decoder[NestingOneOfBase].decode(Encoder[NestingOneOfBase].encode(somethingElse)) should equal(somethingElse)
    }

    "decode to the specified default value if the corresponding field is not found" in {
      Decoder[EvolvedOneOfBase].decode(Encoder[OneOfBase].encode(OneOfBase.Foo("foo"))) should equal(
        EvolvedOneOfBase.Foo("foo")
      )
      Decoder[EvolvedOneOfBase].decode(Encoder[OneOfBase].encode(OneOfBase.Bar(123))) should equal(
        EvolvedOneOfBase.Unknown
      )
    }

    "allow adding new subtype" in {
      Decoder[OneOfBase].decode(Encoder[EvolvedOneOfBase].encode(EvolvedOneOfBase.Baz(12.3d))) should equal(
        OneOfBase.Unknown
      )
    }

    "encode the annotated unknown value and decode back to the same" in {
      Decoder[OneOfBase].decode(Encoder[OneOfBase].encode(OneOfBase.Unknown)) should equal(
        OneOfBase.Unknown
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
