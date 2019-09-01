package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets.UTF_8

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.testkit.{ImplicitSender, TestProbe}
import better.files.File
import dev.chopsticks.fp.{AkkaApp, LoggingContext, ZAkka}
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestCf, TestDb}
import dev.chopsticks.kvdb.codec.KeyConstraints
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.kvdb.util.KvdbSerdesUtils._
import dev.chopsticks.kvdb.util.KvdbUtils._
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest._
import zio.blocking._
import zio.{RIO, Task, UIO}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions

object KvdbDatabaseTest {

  final case class Fixture(db: TestDb)

  def withTempDir[R, A](block: File => RIO[R, A]): RIO[R with Blocking, A] = {
    blocking(Task(File.newTemporaryDirectory().deleteOnExit()))
      .bracket { f =>
        blocking(Task(f.delete())).orDie
      } { tempDir =>
        block(tempDir)
      }
  }

  private def flattenFlow[T] = Flow[Array[T]].mapConcat { b =>
    new scala.collection.immutable.Iterable[T] {
      val iterator: Iterator[T] = b.iterator
    }
  }

  private val collectValuesSink = Flow[KvdbValueBatch]
    .via(flattenFlow)
    .toMat(Sink.seq)(Keep.right)

  private val collectSink = Flow[KvdbBatch]
    .via(flattenFlow)
    .toMat(Sink.seq)(Keep.right)

  private def collectPairs(
    source: Source[KvdbBatch, Any]
  ): RIO[AkkaApp.Env, immutable.Seq[(Array[Byte], Array[Byte])]] = {
    ZAkka.graph(UIO {
      source
        .toMat(collectSink)(Keep.right)
    })
  }

  private def collectValues(
    source: Source[KvdbValueBatch, Any]
  ): RIO[AkkaApp.Env, immutable.Seq[Array[Byte]]] = {
    ZAkka.graph(UIO {
      source
        .toMat(collectValuesSink)(Keep.right)
    })
  }

  private val $ : (KeyConstraints[String] => KeyConstraints[String]) => KvdbKeyConstraintList =
    KeyConstraints.constrain[String]

  private val $$ : (
    KeyConstraints[String] => KeyConstraints[String],
    KeyConstraints[String] => KeyConstraints[String]
  ) => KvdbKeyRange =
    KeyConstraints.range[String]

  implicit def stringToByteArray(s: String): Array[Byte] = s.getBytes(UTF_8)

  private def createTestRunner(
    env: AkkaApp.Env,
    runTest: (TestDb => RIO[AkkaApp.Env, Assertion]) => RIO[AkkaApp.Env, Assertion]
  ): (Fixture => RIO[AkkaApp.Env, Assertion]) => Future[Assertion] = {
    import env._
    (testCode: Fixture => RIO[AkkaApp.Env, Assertion]) => {
      unsafeRunToFuture(runTest { db =>
        db.openTask()
          .flatMap(_ => testCode(Fixture(db)))
      }.provide(env))
    }
  }

  private def populateColumn[CF <: TestCf[K, V], K, V](
    db: TestDb,
    column: CF,
    pairs: Seq[(K, V)]
  ): Task[Unit] = {
    val batch = pairs
      .foldLeft(db.transactionBuilder()) {
        case (tx, (k, v)) =>
          tx.put(column, k, v)
      }
      .result
    db.transactionTask(batch)
  }

  implicit val testKvdbClientOptions: KvdbClientOptions =
    dev.chopsticks.kvdb.util.KvdbUtils.Implicits.defaultClientOptions.copy(tailPollingInterval = 10.millis)
}

abstract private[kvdb] class KvdbDatabaseTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inside
//    with ParallelTestExecution
    with ImplicitSender
    with AkkaTestKitAutoShutDown
    with LoggingContext {
  import KvdbDatabaseTest._

  protected def runTest: (TestDb => RIO[AkkaApp.Env, Assertion]) => RIO[AkkaApp.Env, Assertion]
  protected def defaultCf: DefaultCf
  protected def lookupCf: LookupCf

  private lazy val withFixture = createTestRunner(Environment, runTest)

  private lazy val as = system

  private object Environment extends AkkaApp.LiveEnv {
    implicit val actorSystem: ActorSystem = as
  }

  private def assertPair(pair: Option[KvdbPair], key: String, value: String): Assertion = {
    inside(pair) {
      case Some((k, v)) =>
        byteArrayToString(k) should equal(key)
        byteArrayToString(v) should equal(value)
    }
  }

  private def assertPairs(pairs: Seq[KvdbPair], vs: Seq[(String, String)]): Assertion = {
    pairs.map(p => (byteArrayToString(p._1), byteArrayToString(p._2))) should equal(vs)
  }

  private def assertValues(values: Seq[Array[Byte]], vs: Seq[String]): Assertion = {
    values.map(p => byteArrayToString(p)) should equal(vs)
  }

  import Environment.materializer

  "putTask" should {
    "persist" in withFixture {
      case Fixture(db) =>
        val key = "aaa"
        val value = "bbb"
        for {
          _ <- db.putTask(defaultCf, key, value)
          pair <- db.getTask(defaultCf, $(_ is key))
        } yield {
          inside(pair) {
            case Some((k, v)) =>
              byteArrayToString(k) should equal(key)
              byteArrayToString(v) should equal(value)
          }
        }
    }
  }

  "getTask" when {
    "empty constraint" should {
      "return None" in withFixture {
        case Fixture(db) =>
          for {
            pair <- db.getTask(defaultCf, KeyConstraints.toList(KeyConstraints.seed))
          } yield {
            pair should be(None)
          }
      }
    }

    "exact key match" should {
      "return None if key is not found" in withFixture {
        case Fixture(db) =>
          val key = "aaa"

          for {
            pair <- db.getTask(defaultCf, $(_ is key))
          } yield {
            pair should be(None)
          }
      }

      "return None if exact key is not found" in withFixture {
        case Fixture(db) =>
          val key = "aaa"
          val value = "bbb"
          for {
            _ <- db.putTask(defaultCf, key, value)
            pair <- db.getTask(defaultCf, $(_ is "a"))
          } yield {
            pair should be(None)
          }
      }
    }

    "single constraint" should {
      "[^=] seek and return pair with matching key" in withFixture {
        case Fixture(db) =>
          val key = "aaa"
          val value = "bbb"
          for {
            _ <- db.putTask(defaultCf, key, value)
            pair <- db.getTask(defaultCf, $(_ ^= "a"))
          } yield {
            assertPair(pair, key, value)
          }
      }

      "[^=] seek and return None if key doesn't match prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaaa1" -> "aaaa1",
                "cccc1" -> "cccc1"
              )
            )
            pair <- db.getTask(defaultCf, $(_ ^= "bbbb"))
          } yield {
            pair should be(None)
          }
      }

      "[.first ^=] seek to first and return pair with matching key" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb"
              )
            )
            pair <- db.getTask(defaultCf, $(_.first ^= "a"))
          } yield {
            assertPair(pair, "aaa", "aaa")
          }
      }

      "[>=] seek and return pair the key of which is greater than prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaaa1" -> "aaaa1",
                "cccc1" -> "cccc1"
              )
            )
            pair <- db.getTask(defaultCf, $(_ >= "bbbb"))
          } yield {
            assertPair(pair, "cccc1", "cccc1")
          }
      }

      "[>=] seek and return pair the key of which matches prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaaa1" -> "aaaa1",
                "cccc1" -> "cccc1"
              )
            )
            pair <- db.getTask(defaultCf, $(_ >= "cccc"))
          } yield {
            assertPair(pair, "cccc1", "cccc1")
          }
      }

      "[>=] return None if there's no key greater than or equal to prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaaa1" -> "aaaa1",
                "cccc1" -> "cccc1"
              )
            )
            pair <- db.getTask(defaultCf, $(_ >= "dddd"))
          } yield {
            pair should be(None)
          }
      }

      "[first] seek to first and return None if key doesn't match prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb"
              )
            )
            pair <- db.getTask(defaultCf, $(_.first ^= "b"))
          } yield {
            pair should be(None)
          }
      }

      "[last] seek to last and return pair with matching key" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb"
              )
            )
            pair <- db.getTask(defaultCf, $(_.last ^= "b"))
          } yield {
            assertPair(pair, "bbb", "bbb")
          }
      }

      "[last] seek to last and return None if key doesn't match prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb"
              )
            )
            pair <- db.getTask(defaultCf, $(_.last ^= "a"))
          } yield {
            pair should be(None)
          }
      }
    }

    "combo constraints" should {
      "> exact match + ^= prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ > "bbb" ^= "c")
            )
          } yield {
            assertPair(pair, "ccc", "ccc")
          }
      }

      "> prefix matched + ^= prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ > "bb" ^= "bbb")
            )
          } yield {
            assertPair(pair, "bbb", "bbb")
          }
      }

      "> prefix matched + ^= prefix no match" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ > "b" ^= "d")
            )
          } yield {
            pair should be(None)
          }
      }

      "< exact match + ^= prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ < "bbb" ^= "a")
            )
          } yield {
            assertPair(pair, "aaa", "aaa")
          }
      }

      "< prefix matched + ^= prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ < "b" ^= "a")
            )
          } yield {
            assertPair(pair, "aaa", "aaa")
          }
      }

      "< prefix matched + ^= prefix no match" in withFixture {
        case Fixture(db) =>
          for {
            _ <- populateColumn(
              db,
              defaultCf,
              List(
                "aaa" -> "aaa",
                "bbb" -> "bbb",
                "ccc" -> "ccc"
              )
            )
            pair <- db.getTask(
              defaultCf,
              $(_ < "b" ^= "c")
            )
          } yield {
            pair should be(None)
          }
      }
    }
  }

  "deleteTask" should {
    "do nothing if no such key is found" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.deleteTask(defaultCf, "cccc1")
          all <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
        } yield {
          assertPairs(
            all,
            Vector(
              ("aaaa1", "aaaa1")
            )
          )
        }
    }

    "delete if key is found" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "aaaa2", "aaaa2")
          _ <- db.deleteTask(defaultCf, "aaaa1")
          all <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
        } yield {
          assertPairs(
            all,
            Vector(
              ("aaaa2", "aaaa2")
            )
          )
        }
    }

    "delete if key is found even after being overwritten" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "aaaa2", "aaaa2")
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa3")
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa4")
          _ <- db.deleteTask(defaultCf, "aaaa1")
          all <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
        } yield {
          assertPairs(
            all,
            Vector(
              ("aaaa2", "aaaa2")
            )
          )
        }
    }
  }

  "deletePrefixTask" should {
    "do nothing if no such key is found" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          count <- db.deletePrefixTask(defaultCf, "cccc")
        } yield {
          count should be(0)
        }
    }

    "delete a single matching key" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          count <- db.deletePrefixTask(defaultCf, "aaaa")
          all <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
        } yield {
          count should be(1)
          assertPairs(
            all,
            Vector(
              ("bbbb1", "bbbb1"),
              ("bbbb2", "bbbb2")
            )
          )
        }
    }

    "delete all keys matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "aaaa2", "aaaa2")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          count <- db.deletePrefixTask(defaultCf, "bbbb")
          all <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
        } yield {
          count should be(2)
          assertPairs(
            all,
            Vector(
              ("aaaa1", "aaaa1"),
              ("aaaa2", "aaaa2")
            )
          )
        }
    }

  }

  "batchGetTask" should {
    "return batched result" in withFixture {
      case Fixture(db) =>
        val batchGet = Vector("a", "b", "c", "d").map(
          p => $(_ ^= p)
        )

        for {
          _ <- db.putTask(defaultCf, "aaa", "aaa")
          _ <- db.putTask(defaultCf, "bbb", "bbb")
          _ <- db.putTask(defaultCf, "ccc", "ccc")
          pairs <- db.batchGetTask(defaultCf, batchGet)
        } yield {
          inside(pairs.toVector) {
            case p1 +: p2 +: p3 +: p4 =>
              assertPair(p1, "aaa", "aaa")
              assertPair(p2, "bbb", "bbb")
              assertPair(p3, "ccc", "ccc")
              p4 should be(Vector(None))
          }
        }
    }
  }

  "iterateSource" should {
    "complete with SeekFailure if constraint list is empty" in withFixture {
      case Fixture(db) =>
        collectPairs(db.iterateSource(defaultCf, $$(identity, identity))).either
          .map { ret =>
            ret should matchPattern {
              case Left(_: SeekFailure) =>
            }
          }
    }

    "iterate from matching initial prefix until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "aaaa2", "aaaa2")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          pairs <- collectPairs(db.iterateSource(defaultCf, $$(_ ^= "aaaa", _ ^= "aaaa")))
        } yield {
          assertPairs(
            pairs,
            Vector(
              ("aaaa1", "aaaa1"),
              ("aaaa2", "aaaa2")
            )
          )
        }
    }

    "respect given MaxKvdbBatchBytes" in withFixture {
      case Fixture(db) =>
        val count = 10000
        val pad = 5
        val batchSize = 10000

        for {
          tx <- Task {
            (1 to count)
              .foldLeft(db.transactionBuilder()) { (tx, i) =>
                val padded = s"%0${pad}d".format(i)
                tx.put(defaultCf, padded, padded)
              }
              .result
          }
          _ <- db.transactionTask(tx)
          batches <- ZAkka.graph(UIO {
            db.iterateSource(defaultCf, $$(_.first, _.last))(
                testKvdbClientOptions.copy(maxBatchBytes = batchSize)
              )
              .toMat(Sink.seq)(Keep.right)
          })
        } yield {
          batches.size should equal(10)
        }
    }

    "iterate within a matching range" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          _ <- db.putTask(defaultCf, "bbbb3", "bbbb3")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          pairs <- collectPairs(db.iterateSource(defaultCf, $$(_ >= "bbbb1", _ < "bbbb3")))
        } yield {
          assertPairs(
            pairs,
            Vector(
              ("bbbb1", "bbbb1"),
              ("bbbb2", "bbbb2")
            )
          )
        }
    }

    "iterate within a non-matching range" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          _ <- db.putTask(defaultCf, "bbbb3", "bbbb3")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          ret <- collectPairs(db.iterateSource(defaultCf, $$(_ >= "bbbb", _ < "bbbb1"))).either
        } yield {
          ret should matchPattern {
            case Left(_: SeekFailure) =>
          }
        }
    }

    "iterate from exact matching key until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          pairs <- collectPairs(db.iterateSource(defaultCf, $$(_ is "bbbb1", _ ^= "bbbb")))
        } yield {
          assertPairs(
            pairs,
            Vector(
              ("bbbb1", "bbbb1"),
              ("bbbb2", "bbbb2")
            )
          )
        }
    }

    "iterate from previous key matching initial prefix until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb3", "bbbb3")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          pairs <- collectPairs(db.iterateSource(defaultCf, $$(_ <= "bbbb2", _ ^= "bbbb")))
        } yield {
          assertPairs(
            pairs,
            Vector(
              ("bbbb1", "bbbb1"),
              ("bbbb3", "bbbb3")
            )
          )
        }
    }
  }

  "iterateValuesSource" should {
    "iterate from matching initial prefix until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "aaaa2", "aaaa2")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          values <- collectValues(db.iterateValuesSource(defaultCf, $$(_ ^= "aaaa", _ ^= "aaaa")))
        } yield {
          assertValues(
            values,
            Vector(
              "aaaa1",
              "aaaa2"
            )
          )
        }
    }

    "iterate from exact matching key until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          values <- collectValues(db.iterateValuesSource(defaultCf, $$(_ is "bbbb1", _ ^= "bbbb")))
        } yield {
          assertValues(
            values,
            Vector(
              "bbbb1",
              "bbbb2"
            )
          )
        }
    }

    "iterate from previous key matching initial prefix until the last subsequent matching prefix" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "bbbb3", "bbbb3")
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          values <- collectValues(db.iterateValuesSource(defaultCf, $$(_ < "bbbb2", _ ^= "bbbb")))
        } yield {
          assertValues(
            values,
            Vector(
              "bbbb1",
              "bbbb3"
            )
          )
        }
    }
  }

  /*"writeBatchTask" should {
    "put batch" in withFixture {
      case Fixture(db) =>
        val batch = Vector(
          (defaultCf, "bbbb1", "bbbb1"),
          (TestKvdb.columns.lookup, "aaaa", "aaaa"),
          (defaultCf, "bbbb2", "bbbb2")
        ).map(t => t.copy(_2 = t._2.getBytes(UTF_8), _3 = t._3.getBytes(UTF_8)))

        for {
          _ <- db.batchPutTask(batch)
          pair <- db.getTask(TestKvdb.columns.lookup, $(_ is "aaaa"))
        } yield {
          assertPair(pair, "aaaa", "aaaa")
        }
    }
  }*/

  "tailSource" should {
    "complete with UnsupportedKvdbOperationException if constraint list is empty" in withFixture {
      case Fixture(db) =>
        ZAkka
          .graph(UIO {
            db.tailSource(defaultCf, $$(identity, identity))
              .toMat(Sink.head)(Keep.right)
          })
          .either
          .map { ret =>
            ret should matchPattern {
              case Left(_: UnsupportedKvdbOperationException) =>
            }
          }
    }

    "respect given MaxKvdbBatchBytes" in withFixture {
      case Fixture(db) =>
        val count = 10000
        val pad = 5
        val batchSize = 10000

        for {
          tx <- Task {
            (1 to count)
              .foldLeft(db.transactionBuilder()) { (tx, i) =>
                val padded = s"%0${pad}d".format(i)
                tx.put(defaultCf, padded, padded)
              }
              .result
          }
          _ <- db.transactionTask(tx)
          batches <- ZAkka
            .graph(UIO {
              db.tailSource(defaultCf, $$(_.first, _.last))(
                  testKvdbClientOptions.copy(maxBatchBytes = batchSize)
                )
                .takeWhile(
                  (b: KvdbTailBatch) => b.right.forall(a => KvdbSerdesUtils.byteArrayToString(a.last._1) != "10000"),
                  inclusive = true
                )
                .toMat(Sink.seq)(Keep.right)
            })
        } yield {
          batches.size should equal(10)
        }
    }

    "tail" in withFixture {
      case Fixture(db) =>
        val source = db
          .tailSource(defaultCf, $$(_ ^= "bbbb", _ ^= "bbbb"))
          .collect { case Right(b) => b }
          .via(flattenFlow)
        val probe = TestProbe()
        val ks = source
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(probe.ref, "completed"))
          .run()

        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- Task {
            val pair = probe.expectMsgPF(3.seconds) {
              case p => p.asInstanceOf[KvdbPair]
            }
            byteArrayToString(pair._1) should equal("bbbb1")
            byteArrayToString(pair._2) should equal("bbbb1")
          }
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          lastPair <- Task(probe.expectMsgPF(3.seconds) {
            case p => p.asInstanceOf[KvdbPair]
          })
        } yield {
          ks.shutdown()
          probe.expectMsg(300.millis, "completed")
          byteArrayToString(lastPair._1) should equal("bbbb2")
          byteArrayToString(lastPair._2) should equal("bbbb2")
        }
    }

    "tail last when empty" in withFixture {
      case Fixture(db) =>
        val source = db
          .tailSource(defaultCf, $$(_.last, _.last))
          .collect { case Right(b) => b }
          .via(flattenFlow)
        val probe = TestProbe()

        source
          .take(1)
          .runWith(Sink.actorRef(probe.ref, "completed"))

        for {
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "aaaa", "aaaa")
          pair <- Task {
            probe.expectMsgPF(1.second) {
              case p => p.asInstanceOf[KvdbPair]
            }
          }
        } yield {
          probe.expectMsg(300.millis, "completed")
          byteArrayToString(pair._1) should equal("aaaa")
          byteArrayToString(pair._2) should equal("aaaa")
        }
    }

    "tail last when not empty" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa", "aaaa")
          head <- ZAkka
            .graph(UIO {
              db.tailSource(defaultCf, $$(_.last, _.last))
                .collect { case Right(b) => b }
                .completionTimeout(1.second)
                .toMat(Sink.head)(Keep.right)
            })
            .map(_.head)
        } yield {
          byteArrayToString(head._1) should equal("aaaa")
          byteArrayToString(head._2) should equal("aaaa")
        }
    }
  }

  "batchTailSource" should {
    "complete with UnsupportedKvdbOperationException if constraint list is empty" in withFixture {
      case Fixture(db) =>
        ZAkka
          .graph(UIO {
            db.batchTailSource(defaultCf, List.empty)
              .toMat(Sink.head)(Keep.right)
          })
          .either
          .map { ret =>
            ret should matchPattern {
              case Left(_: UnsupportedKvdbOperationException) =>
            }
          }
    }

    "tail" in withFixture {
      case Fixture(db) =>
        val source = db
          .batchTailSource(
            defaultCf,
            List(
              $$(_ ^= "aaaa", _ ^= "aaaa"),
              $$(_ ^= "bbbb", _ ^= "bbbb")
            )
          )
          .collect {
            case (index, Right(b)) => (index, b.map(p => byteArrayToString(p._1) -> byteArrayToString(p._2)).toVector)
          }

        val probe = TestProbe()
        val ks = source
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(probe.ref, "completed"))
          .run()

        for {
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- Task {
            val message = probe.expectMsgPF(3.seconds) {
              case p => p.asInstanceOf[(Int, KvdbPair)]
            }
            message should equal((1, Vector("bbbb1" -> "bbbb1")))
          }
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          lastMessage <- Task(probe.expectMsgPF(3.seconds) {
            case p => p.asInstanceOf[(Int, KvdbPair)]
          })
        } yield {
          ks.shutdown()
          probe.expectMsg(300.millis, "completed")
          lastMessage should equal((0, Vector("aaaa1" -> "aaaa1")))
        }
    }
  }

  "tailValuesSource" should {
    "tail" in withFixture {
      case Fixture(db) =>
        val source = db
          .tailValuesSource(defaultCf, $$(_ ^= "bbbb", _ ^= "bbbb"))
          .collect { case Right(b) => b }
          .via(flattenFlow)
        val probe = TestProbe()
        val ks = source
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(probe.ref, "completed"))
          .run()

        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "bbbb1", "bbbb1")
          _ <- Task {
            val value = probe.expectMsgPF(3.seconds) {
              case p => p.asInstanceOf[Array[Byte]]
            }
            byteArrayToString(value) should equal("bbbb1")
          }
          _ <- db.putTask(defaultCf, "cccc1", "cccc1")
          _ <- Task(probe.expectNoMessage(100.millis))
          _ <- db.putTask(defaultCf, "bbbb2", "bbbb2")
          lastValue <- Task(probe.expectMsgPF(3.seconds) {
            case p => p.asInstanceOf[Array[Byte]]
          })
        } yield {
          ks.shutdown()
          probe.expectMsg(300.millis, "completed")
          byteArrayToString(lastValue) should equal("bbbb2")
        }
    }
  }

  "transactionTask" should {
    "maintain atomicity" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.putTask(defaultCf, "aaaa1", "aaaa1")
          _ <- db.putTask(defaultCf, "pppp1", "pppp1")
          _ <- db.putTask(lookupCf, "bbbb1", "bbbb1")
          _ <- db.putTask(defaultCf, "pppp2", "pppp2")
          _ <- db.transactionTask(
            db.transactionBuilder()
              .delete(defaultCf, "aaaa1")
              .put(lookupCf, "dddd1", "dddd1")
              .delete(lookupCf, "bbbb1")
              .put(defaultCf, "cccc1", "cccc1")
              .deletePrefix(defaultCf, "pppp")
              .result
          )
          allDefault <- collectPairs(db.iterateSource(defaultCf, $$(_.first, _.last)))
          allLookup <- collectPairs(db.iterateSource(lookupCf, $$(_.first, _.last)))
        } yield {
          assertPairs(
            allDefault,
            Vector(
              ("cccc1", "cccc1")
            )
          )
          assertPairs(
            allLookup,
            Vector(
              ("dddd1", "dddd1")
            )
          )
        }
    }
  }

  "statsTask" should {
    "return stats" in withFixture {
      case Fixture(db) =>
        for {
          stats <- db.statsTask
        } yield {
          assert(stats.nonEmpty)
        }
    }

    "throw KvdbAlreadyClosedException if the (local) db is already closed" in withFixture {
      case Fixture(db) =>
        for {
          _ <- db.closeTask()
          correctBehavior <- db.statsTask
            .map(_ => !db.isLocal)
            .catchAll {
              case _: KvdbAlreadyClosedException => UIO.succeed(true)
              case _ => UIO.succeed(false)
            }
        } yield {
          correctBehavior should equal(true)
        }
    }
  }
}
