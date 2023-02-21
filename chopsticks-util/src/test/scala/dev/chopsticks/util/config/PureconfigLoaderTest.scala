//package dev.chopsticks.util.config
//
//import com.typesafe.config.ConfigFactory
//import org.scalatest.{Assertions, Inside}
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpecLike
//import pureconfig.ConfigReader
//import pureconfig.generic.ProductHint
//
//object PureconfigLoaderTest {
//  final case class Foo(bar: String, baz: Boolean)
//}
//
//final class PureconfigLoaderTest extends AnyWordSpecLike with Assertions with Matchers with Inside {
//  import PureconfigLoaderTest._
//
//  "load with a default hint" in {
//    //noinspection TypeAnnotation
//    implicit val configReader = {
//      import dev.chopsticks.util.config.PureconfigConverters._
//      ConfigReader[Foo]
//    }
//
//    val result = PureconfigLoader.load[Foo](
//      ConfigFactory.parseString(
//        """
//        |ns {
//        |  bar = bar
//        |  baz = true
//        |  extra = 123
//        |}
//        |""".stripMargin
//      ),
//      "ns"
//    )
//
//    result.isLeft should be(true)
//  }
//
//  "load with a custom hint" in {
//    //noinspection TypeAnnotation
//    implicit val configReader = {
//      import dev.chopsticks.util.config.PureconfigConverters._
//      implicit val hint: ProductHint[Foo] = ProductHint[Foo](allowUnknownKeys = true)
//      ConfigReader[Foo]
//    }
//
//    val result = PureconfigLoader.load[Foo](
//      ConfigFactory.parseString(
//        """
//        |ns {
//        |  bar = bar
//        |  baz = true
//        |  extra = 123
//        |}
//        |""".stripMargin
//      ),
//      "ns"
//    )
//
//    result should equal(Right(Foo("bar", baz = true)))
//  }
//}
