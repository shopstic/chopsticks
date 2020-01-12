package dev.chopsticks.kvdb.codec

import cats.syntax.show._
import dev.chopsticks.kvdb.codec.berkeleydb_key._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object KeyConstraintsTest {
  final case class StockDbKeyTest(symbol: String, year: Int, month: Int)
  object StockDbKeyTest {
    //noinspection TypeAnnotation
    implicit val dbKey = KeySerdes[StockDbKeyTest]
  }
}

final class KeyConstraintsTest extends AnyWordSpecLike with Matchers {
  import KeyConstraintsTest._
  import KeyConstraints.Implicits._

  "work" in {
    implicitly[KeyPrefix[String, StockDbKeyTest]]

    println(KeyConstraints.range[StockDbKeyTest](_ is StockDbKeyTest("AAPL", 2017, 3), _ ^= "AAPL").show)
    println(KeyConstraints.range[StockDbKeyTest](_ >= "AAPL123" ^= "AAPL", _ <= "AAPL456").show)
    println(KeyConstraints.range[StockDbKeyTest](_ >= ("AAPL123" -> 2017) ^= "AAPL", _ <= "AAPL456").show)
    println(KeyConstraints.range[StockDbKeyTest](_ <= "AAPL123" ^= "AAPL", _ < "AAPL999").show)
    println(KeyConstraints.range[StockDbKeyTest](_.first, _.last).show)
  }
}
