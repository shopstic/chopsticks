package dev.chopsticks.kvdb.codec

import org.scalatest.{Matchers, WordSpecLike}
import shapeless._
import cats.syntax.show._
import dev.chopsticks.kvdb.codec.berkeleydb_key._

object DbKeyConstraintsTest {
  final case class StockDbKeyTest(symbol: String, year: Int, month: Int)
  object StockDbKeyTest {
    //noinspection TypeAnnotation
    implicit val dbKey = DbKey[StockDbKeyTest]
  }
}

class DbKeyConstraintsTest extends WordSpecLike with Matchers {
  import DbKeyConstraintsTest._
  import DbKeyConstraints.Implicits._

  "work" in {
    println(DbKeyConstraints.range[StockDbKeyTest](_ is StockDbKeyTest("AAPL", 2017, 3), _ ^= "AAPL").show)
    println(DbKeyConstraints.range[StockDbKeyTest](_ >= "AAPL123" ^= "AAPL", _ <= "AAPL456").show)
    println(DbKeyConstraints.range[StockDbKeyTest](_ <= "AAPL123" ^= "AAPL", _ < "AAPL999").show)
    println(DbKeyConstraints.range[StockDbKeyTest](_ ^= "AAPL" :: 2017 :: HNil, _ <= "AAPL" :: 2019 :: 9 :: HNil).show)
    println(DbKeyConstraints.range[StockDbKeyTest](_.first, _.last).show)
  }
}
