package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets.UTF_8

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.testkit.{ImplicitSender, TestProbe}
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.codec.DbKeyConstraints
import dev.chopsticks.kvdb.proto.{DbKeyConstraintList, DbKeyRange}
import dev.chopsticks.kvdb.util.DbUtils._
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.kvdb.util.KvdbSerdesUtils._
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest._
import zio.blocking._
import zio.clock.Clock
import zio.{Task, RIO, UIO}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions
import dev.chopsticks.kvdb.codec.primitive._

object DbTest extends StrictLogging {

  final case class Fixture(db: DbInterface[TestDb.type])

  def withTempDir[R, A](block: File => RIO[R, A]): RIO[R with Blocking, A] = {
    blocking(Task(File.newTemporaryDirectory().deleteOnExit()))
      .bracket(f => blocking(UIO(f.delete()))) { tempDir =>
        block(tempDir)
      }
  }

  private def flattenFlow[T] = Flow[Array[T]].mapConcat { b =>
    new scala.collection.immutable.Iterable[T] {
      val iterator: Iterator[T] = b.iterator
    }
  }

  private val collectValuesSink = Flow[DbValueBatch]
    .via(flattenFlow)
    .toMat(Sink.seq)(Keep.right)
  private val collectSink = Flow[DbBatch]
    .via(flattenFlow)
    .toMat(Sink.seq)(Keep.right)

  private val $ : (DbKeyConstraints[String] => DbKeyConstraints[String]) => DbKeyConstraintList =
    DbKeyConstraints.constrain[String]
  private val $$ : (
    DbKeyConstraints[String] => DbKeyConstraints[String],
    DbKeyConstraints[String] => DbKeyConstraints[String]
  ) => DbKeyRange =
    DbKeyConstraints.range[String]

  implicit def stringToByteArray(s: String): Array[Byte] = s.getBytes(UTF_8)

  private def populateColumn[K, V](
    db: DbInterface[TestDb.type],
    column: TestDbColumn[K, V],
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

  implicit val testDbClientOptions: DbClientOptions =
    dev.chopsticks.kvdb.util.DbUtils.Implicits.defaultDbClientOptions.copy(tailPollingInterval = 10.millis)

  abstract private[kvdb] class DbTest
      extends AkkaTestKit
      with AsyncWordSpecLike
      with Matchers
      with Inside
      //      with ParallelTestExecution
      with ImplicitSender
      with AkkaTestKitAutoShutDown {

    private val as = system

    object Environment extends AkkaApp.LiveEnv {
      implicit val actorSystem: ActorSystem = as
    }

    import Environment.{materializer, unsafeRunToFuture}

    protected def runTest: (DbInterface[TestDb.type] => Task[Assertion]) => RIO[AkkaApp.Env, Assertion]

    private def withFixture(testCode: Fixture => Task[Assertion]): Future[Assertion] = {
      unsafeRunToFuture(runTest { db =>
        db.openTask()
          .flatMap(_ => testCode(Fixture(db)))
      }.provide(Environment))
    }

    private def assertPair(pair: Option[DbPair], key: String, value: String): Assertion = {
      inside(pair) {
        case Some((k, v)) =>
          byteArrayToString(k) should equal(key)
          byteArrayToString(v) should equal(value)
      }
    }

    private def assertPairs(pairs: Seq[DbPair], vs: Seq[(String, String)]): Assertion = {
      pairs.map(p => (byteArrayToString(p._1), byteArrayToString(p._2))) should equal(vs)
    }

    private def assertValues(values: Seq[Array[Byte]], vs: Seq[String]): Assertion = {
      values.map(p => byteArrayToString(p)) should equal(vs)
    }

    "putTask" should {
      "persist" in withFixture {
        case Fixture(db) =>
          val key = "aaa"
          val value = "bbb"
          for {
            _ <- db.putTask(TestDb.columns.Default, key, value)
            pair <- db.getTask(TestDb.columns.Default, $(_ is key))
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
              pair <- db.getTask(TestDb.columns.Default, DbKeyConstraints.toList(DbKeyConstraints.seed))
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
              pair <- db.getTask(TestDb.columns.Default, $(_ is key))
            } yield {
              pair should be(None)
            }
        }

        "return None if exact key is not found" in withFixture {
          case Fixture(db) =>
            val key = "aaa"
            val value = "bbb"
            for {
              _ <- db.putTask(TestDb.columns.Default, key, value)
              pair <- db.getTask(TestDb.columns.Default, $(_ is "a"))
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
              _ <- db.putTask(TestDb.columns.Default, key, value)
              pair <- db.getTask(TestDb.columns.Default, $(_ ^= "a"))
            } yield {
              assertPair(pair, key, value)
            }
        }

        "[^=] seek and return None if key doesn't match prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaaa1" -> "aaaa1",
                  "cccc1" -> "cccc1"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_ ^= "bbbb"))
            } yield {
              pair should be(None)
            }
        }

        "[.first ^=] seek to first and return pair with matching key" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_.first ^= "a"))
            } yield {
              assertPair(pair, "aaa", "aaa")
            }
        }

        "[>=] seek and return pair the key of which is greater than prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaaa1" -> "aaaa1",
                  "cccc1" -> "cccc1"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_ >= "bbbb"))
            } yield {
              assertPair(pair, "cccc1", "cccc1")
            }
        }

        "[>=] seek and return pair the key of which matches prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaaa1" -> "aaaa1",
                  "cccc1" -> "cccc1"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_ >= "cccc"))
            } yield {
              assertPair(pair, "cccc1", "cccc1")
            }
        }

        "[>=] return None if there's no key greater than or equal to prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaaa1" -> "aaaa1",
                  "cccc1" -> "cccc1"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_ >= "dddd"))
            } yield {
              pair should be(None)
            }
        }

        "[first] seek to first and return None if key doesn't match prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_.first ^= "b"))
            } yield {
              pair should be(None)
            }
        }

        "[last] seek to last and return pair with matching key" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_.last ^= "b"))
            } yield {
              assertPair(pair, "bbb", "bbb")
            }
        }

        "[last] seek to last and return None if key doesn't match prefix" in withFixture {
          case Fixture(db) =>
            for {
              _ <- populateColumn(
                db,
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb"
                )
              )
              pair <- db.getTask(TestDb.columns.Default, $(_.last ^= "a"))
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
                TestDb.columns.Default,
                List(
                  "aaa" -> "aaa",
                  "bbb" -> "bbb",
                  "ccc" -> "ccc"
                )
              )
              pair <- db.getTask(
                TestDb.columns.Default,
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.deleteTask(TestDb.columns.Default, "cccc1")
            all <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa2", "aaaa2")
            _ <- db.deleteTask(TestDb.columns.Default, "aaaa1")
            all <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa2", "aaaa2")
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa3")
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa4")
            _ <- db.deleteTask(TestDb.columns.Default, "aaaa1")
            all <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            count <- db.deletePrefixTask(TestDb.columns.Default, "cccc")
          } yield {
            count should be(0)
          }
      }

      "delete a single matching key" in withFixture {
        case Fixture(db) =>
          for {
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            count <- db.deletePrefixTask(TestDb.columns.Default, "aaaa")
            all <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa2", "aaaa2")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            count <- db.deletePrefixTask(TestDb.columns.Default, "bbbb")
            all <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaa", "aaa")
            _ <- db.putTask(TestDb.columns.Default, "bbb", "bbb")
            _ <- db.putTask(TestDb.columns.Default, "ccc", "ccc")
            pairs <- db.batchGetTask(TestDb.columns.Default, batchGet)
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
          Task
            .fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(identity, identity))
                .runWith(collectSink)
            }
            .either
            .map { ret =>
              ret should matchPattern {
                case Left(_: SeekFailure) =>
              }
            }
      }

      "iterate from matching initial prefix until the last subsequent matching prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa2", "aaaa2")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            pairs <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_ ^= "aaaa", _ ^= "aaaa"))
                .runWith(collectSink)
            }
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

      "respect given MaxDbBatchBytes" in withFixture {
        case Fixture(db) =>
          val count = 10000
          val pad = 5
          val batchSize = 10000

          for {
            tx <- Task {
              (1 to count)
                .foldLeft(db.transactionBuilder()) { (tx, i) =>
                  val padded = s"%0${pad}d".format(i)
                  tx.put(_.Default, padded, padded)
                }
                .result
            }
            _ <- db.transactionTask(tx)
            batches <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))(
                  testDbClientOptions.copy(maxBatchBytes = batchSize)
                )
                .runWith(Sink.seq)
            }
          } yield {
            batches.size should equal(10)
          }
      }

      "iterate within a matching range" in withFixture {
        case Fixture(db) =>
          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            _ <- db.putTask(TestDb.columns.Default, "bbbb3", "bbbb3")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            pairs <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_ >= "bbbb1", _ < "bbbb3"))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            _ <- db.putTask(TestDb.columns.Default, "bbbb3", "bbbb3")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            ret <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_ >= "bbbb", _ < "bbbb1"))
                .runWith(collectSink)
            }.either
          } yield {
            ret should matchPattern {
              case Left(_: SeekFailure) =>
            }
          }
      }

      "iterate from exact matching key until the last subsequent matching prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            pairs <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_ is "bbbb1", _ ^= "bbbb"))
                .runWith(collectSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb3", "bbbb3")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            pairs <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_ <= "bbbb2", _ ^= "bbbb"))
                .runWith(collectSink)
            }
          } yield {
            val a: Assertion = assertPairs(
              pairs,
              Vector(
                ("bbbb1", "bbbb1"),
                ("bbbb3", "bbbb3")
              )
            )
            logger.info(s"Done: $a")
            a
          }
      }
    }

    "iterateValuesSource" should {
      "iterate from matching initial prefix until the last subsequent matching prefix" in withFixture {
        case Fixture(db) =>
          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "aaaa2", "aaaa2")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            values <- Task.fromFuture { _ =>
              db.iterateValuesSource(TestDb.columns.Default, $$(_ ^= "aaaa", _ ^= "aaaa"))
                .runWith(collectValuesSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            values <- Task.fromFuture { _ =>
              db.iterateValuesSource(TestDb.columns.Default, $$(_ is "bbbb1", _ ^= "bbbb"))
                .runWith(collectValuesSink)
            }
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "bbbb3", "bbbb3")
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            values <- Task.fromFuture { _ =>
              db.iterateValuesSource(TestDb.columns.Default, $$(_ < "bbbb2", _ ^= "bbbb"))
                .runWith(collectValuesSink)
            }
          } yield {
            val a: Assertion = assertValues(
              values,
              Vector(
                "bbbb1",
                "bbbb3"
              )
            )
            logger.info(s"Done: $a")
            a
          }
      }
    }

    /*"writeBatchTask" should {
      "put batch" in withFixture {
        case Fixture(db) =>
          val batch = Vector(
            (TestDb.columns.default, "bbbb1", "bbbb1"),
            (TestDb.columns.lookup, "aaaa", "aaaa"),
            (TestDb.columns.default, "bbbb2", "bbbb2")
          ).map(t => t.copy(_2 = t._2.getBytes(UTF_8), _3 = t._3.getBytes(UTF_8)))

          for {
            _ <- db.batchPutTask(batch)
            pair <- db.getTask(TestDb.columns.lookup, $(_ is "aaaa"))
          } yield {
            assertPair(pair, "aaaa", "aaaa")
          }
      }
    }*/

    "tailSource" should {
      "complete with UnsupportedDbOperationException if constraint list is empty" in withFixture {
        case Fixture(db) =>
          Task
            .fromFuture { _ =>
              db.tailSource(TestDb.columns.Default, $$(identity, identity))
                .runWith(Sink.head)
            }
            .either
            .map { ret =>
              ret should matchPattern {
                case Left(_: UnsupportedDbOperationException) =>
              }
            }
      }

      "respect given MaxDbBatchBytes" in withFixture {
        case Fixture(db) =>
          val count = 10000
          val pad = 5
          val batchSize = 10000

          for {
            tx <- Task {
              (1 to count)
                .foldLeft(db.transactionBuilder()) { (tx, i) =>
                  val padded = s"%0${pad}d".format(i)
                  tx.put(_.Default, padded, padded)
                }
                .result
            }
            _ <- db.transactionTask(tx)
            batches <- Task.fromFuture { _ =>
              db.tailSource(TestDb.columns.Default, $$(_.first, _.last))(
                  testDbClientOptions.copy(maxBatchBytes = batchSize)
                )
                .takeWhile(
                  (b: DbTailBatch) => b.right.forall(a => KvdbSerdesUtils.byteArrayToString(a.last._1) != "10000"),
                  inclusive = true
                )
                .runWith(Sink.seq)
            }
          } yield {
            batches.size should equal(10)
          }
      }

      "tail" in withFixture {
        case Fixture(db) =>
          val source = db
            .tailSource(TestDb.columns.Default, $$(_ ^= "bbbb", _ ^= "bbbb"))
            .collect { case Right(b) => b }
            .via(flattenFlow)
          val probe = TestProbe()
          val ks = source
            .viaMat(KillSwitches.single)(Keep.right)
            .to(Sink.actorRef(probe.ref, "completed"))
            .run()

          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- Task {
              val pair = probe.expectMsgPF(3.seconds) {
                case p => p.asInstanceOf[DbPair]
              }
              byteArrayToString(pair._1) should equal("bbbb1")
              byteArrayToString(pair._2) should equal("bbbb1")
            }
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
            lastPair <- Task(probe.expectMsgPF(3.seconds) {
              case p => p.asInstanceOf[DbPair]
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
            .tailSource(TestDb.columns.Default, $$(_.last, _.last))
            .collect { case Right(b) => b }
            .via(flattenFlow)
          val probe = TestProbe()

          source
            .take(1)
            .runWith(Sink.actorRef(probe.ref, "completed"))

          for {
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "aaaa", "aaaa")
            pair <- Task {
              probe.expectMsgPF(1.second) {
                case p => p.asInstanceOf[DbPair]
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa", "aaaa")
            head <- Task.fromFuture { _ =>
              db.tailSource(TestDb.columns.Default, $$(_.last, _.last))
                .collect { case Right(b) => b }
                .completionTimeout(1.second)
                .runWith(Sink.head)
                .map(_.head)
            }
          } yield {
            byteArrayToString(head._1) should equal("aaaa")
            byteArrayToString(head._2) should equal("aaaa")
          }
      }
    }

    "batchTailSource" should {
      "complete with UnsupportedDbOperationException if constraint list is empty" in withFixture {
        case Fixture(db) =>
          Task
            .fromFuture { _ =>
              db.batchTailSource(TestDb.columns.Default, List.empty)
                .runWith(Sink.head)
            }
            .either
            .map { ret =>
              ret should matchPattern {
                case Left(_: UnsupportedDbOperationException) =>
              }
            }
      }

      "tail" in withFixture {
        case Fixture(db) =>
          val source = db
            .batchTailSource(
              TestDb.columns.Default,
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
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- Task {
              val message = probe.expectMsgPF(3.seconds) {
                case p => p.asInstanceOf[(Int, DbPair)]
              }
              message should equal((1, Vector("bbbb1" -> "bbbb1")))
            }
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            lastMessage <- Task(probe.expectMsgPF(3.seconds) {
              case p => p.asInstanceOf[(Int, DbPair)]
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
            .tailValuesSource(TestDb.columns.Default, $$(_ ^= "bbbb", _ ^= "bbbb"))
            .collect { case Right(b) => b }
            .via(flattenFlow)
          val probe = TestProbe()
          val ks = source
            .viaMat(KillSwitches.single)(Keep.right)
            .to(Sink.actorRef(probe.ref, "completed"))
            .run()

          for {
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "bbbb1", "bbbb1")
            _ <- Task {
              val value = probe.expectMsgPF(3.seconds) {
                case p => p.asInstanceOf[Array[Byte]]
              }
              byteArrayToString(value) should equal("bbbb1")
            }
            _ <- db.putTask(TestDb.columns.Default, "cccc1", "cccc1")
            _ <- Task(probe.expectNoMessage(100.millis))
            _ <- db.putTask(TestDb.columns.Default, "bbbb2", "bbbb2")
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
            _ <- db.putTask(TestDb.columns.Default, "aaaa1", "aaaa1")
            _ <- db.putTask(TestDb.columns.Default, "pppp1", "pppp1")
            _ <- db.putTask(TestDb.columns.Lookup, "bbbb1", "bbbb1")
            _ <- db.putTask(TestDb.columns.Default, "pppp2", "pppp2")
            _ <- db.transactionTask(
              db.transactionBuilder()
                .delete(_.Default, "aaaa1")
                .put(_.Lookup, "dddd1", "dddd1")
                .delete(_.Lookup, "bbbb1")
                .put(_.Default, "cccc1", "cccc1")
                .deletePrefix(_.Default, "pppp")
                .result
            )
            allDefault <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Default, $$(_.first, _.last))
                .runWith(collectSink)
            }
            allLookup <- Task.fromFuture { _ =>
              db.iterateSource(TestDb.columns.Lookup, $$(_.first, _.last))
                .runWith(collectSink)
            }
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

      "throw DbAlreadyClosedException if the (local) db is already closed" in withFixture {
        case Fixture(db) =>
          val task: RIO[Blocking with Clock, Assertion] = for {
            _ <- db.closeTask()
            correctBehavior <- db.statsTask
              .map(_ => !db.isLocal)
              .catchAll {
                case _: DbAlreadyClosedException => UIO.succeed(true)
                case _ => UIO.succeed(false)
              }
          } yield {
            correctBehavior should equal(true)
          }

          task.provide(Environment)
      }
    }
  }

}
