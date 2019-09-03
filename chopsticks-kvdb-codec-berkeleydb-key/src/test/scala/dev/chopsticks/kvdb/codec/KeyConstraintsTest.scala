package dev.chopsticks.kvdb.codec

import org.scalatest.{Matchers, WordSpecLike}
import cats.syntax.show._
import dev.chopsticks.kvdb.codec.berkeleydb_key._

object KeyConstraintsTest {
  final case class StockDbKeyTest(symbol: String, year: Int, month: Int)
  object StockDbKeyTest {
    //noinspection TypeAnnotation
    implicit val dbKey = KeySerdes[StockDbKeyTest]
  }
}

final class KeyConstraintsTest extends WordSpecLike with Matchers {
  import KeyConstraintsTest._
  import KeyConstraints.Implicits._

  "work" in {
    println(KeyConstraints.range[StockDbKeyTest](_ is StockDbKeyTest("AAPL", 2017, 3), _ ^= "AAPL").show)
    println(KeyConstraints.range[StockDbKeyTest](_ >= "AAPL123" ^= "AAPL", _ <= "AAPL456").show)
    println(KeyConstraints.range[StockDbKeyTest](_ <= "AAPL123" ^= "AAPL", _ < "AAPL999").show)
    println(KeyConstraints.range[StockDbKeyTest](_.first, _.last).show)
  }
}
