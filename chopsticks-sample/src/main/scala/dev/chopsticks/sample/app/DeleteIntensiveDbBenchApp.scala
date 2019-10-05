package dev.chopsticks.sample.app

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import akka.NotUsed
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.config.{Config => TypesafeConfig}
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaApp, AkkaEnv, LogEnv}
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase
import dev.chopsticks.kvdb.util.KvdbClientOptions.Implicits.defaultClientOptions
import dev.chopsticks.kvdb.util.KvdbException.SeekFailure
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import dev.chopsticks.stream.ZAkkaStreams
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.refineV
import zio._
import zio.clock.Clock

import scala.concurrent.duration._
import scala.util.Random

object DeleteIntensiveDbBenchApp extends AkkaApp {
  final case class MtMessageId(uuid: UUID) extends AnyVal
  final case class ScheduleKey(time: Instant, id: MtMessageId)

  object DbDef extends KvdbDefinition {
    trait Queue extends BaseCf[ScheduleKey, Unit]
    trait Fact extends BaseCf[MtMessageId, Array[Byte]]
    trait Inflight extends BaseCf[MtMessageId, Unit]

    type CfSet = Queue

    trait Materialization extends KvdbMaterialization[BaseCf, CfSet] {
      def queue: Queue
      def fact: Fact
      def inflight: Inflight
      val columnFamilySet: ColumnFamilySet[BaseCf, CfSet] = ColumnFamilySet[BaseCf]
        .of(queue)
        .and(fact)
        .and(inflight)
    }
  }

  private val genValue = Random.alphanumeric.take(250).mkString("").getBytes(UTF_8)

  private val dbMat = DeleteIntensiveDbBenchMat

  type Env = AkkaApp.Env

  protected def createEnv(untypedConfig: TypesafeConfig): ZManaged[Env, Nothing, Env] =
    ZManaged.environment[AkkaApp.Env]

  protected def createPopulateSource(
    dbApi: KvdbDatabaseApi[DbDef.BaseCf],
    batchCounter: LongAdder,
    counter: LongAdder,
    parallelism: Int
  ) = {
    ZIO.access[AkkaEnv](_.akkaService).map { env =>
      val random = ThreadLocalRandom.current()

      Source
        .fromIterator(() => {
          Iterator continually {
            ScheduleKey(time = Instant.now.plusSeconds(random.nextLong(0L, 3600L)), MtMessageId(UUID.randomUUID()))
          }
        })
        .groupedWithin(2048, 10.millis)
        .wireTap(_ => batchCounter.increment())
        .mapAsyncUnordered(parallelism) { batch =>
          env.unsafeRunToFuture {
            dbApi
              .transactionTask(
                batch
                  .foldLeft(dbApi.transactionBuilder) { (tx, item: ScheduleKey) =>
                    tx.put(dbMat.queue, item, ())
                      .put(dbMat.fact, item.id, genValue)
                  }
                  .result
              )
              .as(batch)
          }
        }
        .wireTap(b => counter.add(b.size.toLong))
    }
  }

  protected def createCompactionFlow(
    db: RocksdbDatabase[DbDef.BaseCf, DbDef.CfSet]
  ): ZIO[AkkaEnv with Clock, Nothing, Flow[Any, duration.Duration, NotUsed]] = {
    ZIO.access[AkkaEnv with Clock] { env =>
      Flow[Any]
        .conflate(Keep.right)
        .mapAsync(1) { _ =>
          env.akkaService.unsafeRunToFuture(
            db.compactRange(dbMat.queue)
              .timed
              .map(_._1)
              .provide(env)
          )
        }
    }
  }

  protected def measure[R, E, A](f: ZIO[R, E, A], metric: LongAdder): ZIO[R with Clock, E, A] = {
    f.timed.map {
      case (d, r) =>
        metric.add(d.toMillis)
        r
    }
  }

  trait DequeueMetrics {
    def batches: LongAdder
    def cycles: LongAdder
    def emptyCycles: LongAdder

    def duration: LongAdder
    def purgeDuration: LongAdder
    def getDuration: LongAdder
  }

  trait ResponseMetrics {
    def batches: LongAdder
    def duration: LongAdder
  }

  protected def createResponseSource(
    dbApi: KvdbDatabaseApi[DbDef.BaseCf],
    metrics: ResponseMetrics,
    parallelism: Int
  ) = {
    val inflightCf = dbApi.columnFamily(dbMat.inflight)

    for {
      flow <- ZAkkaStreams.mapAsyncUnorderedM(parallelism) { batch: Seq[(MtMessageId, Unit)] =>
        val task = for {
          tx <- UIO {
            Random
              .shuffle(batch)
              .foldLeft(dbApi.transactionBuilder) {
                case (t, (k, _)) =>
                  t.delete(dbMat.inflight, k)
              }
              .result
          }
          _ <- dbApi.transactionTask(tx)
        } yield {
          metrics.batches.increment()
          batch
        }

        measure(task, metrics.duration)
      }
    } yield {
      Source
        .repeat(())
        .flatMapConcat(
          _ =>
            inflightCf.source
              .groupedWithin(2048, 10.millis)
              .recover {
                case _: SeekFailure => Seq.empty
              }
              .via(flow)
        )
    }
  }

  protected def createDequeueFlow(
    dbApi: KvdbDatabaseApi[DbDef.BaseCf],
    metrics: DequeueMetrics,
    parallelism: Int
  ) = {
    ZIO.access[AkkaEnv with Clock] { env =>
      Flow[Seq[ScheduleKey]]
        .batchWeighted(100000L, _.size.toLong, identity)(Keep.right)
        .flatMapConcat { _ =>
          metrics.cycles.increment()
          dbApi
            .columnFamily(dbMat.queue)
            .batchedSource(_.first, _.last)
            .recover {
              case _: SeekFailure =>
                List.empty
            }
        }
        .filter {
          case b if b.isEmpty =>
            metrics.emptyCycles.increment()
            false
          case _ => true
        }
        .wireTap(_ => metrics.batches.increment())
        .mapAsyncUnordered(parallelism) { batch: List[(ScheduleKey, Unit)] =>
          val deleteTask = for {
            tx <- UIO {
              batch
                .foldLeft(dbApi.transactionBuilder) {
                  case (tx, (k, _)) =>
                    tx.delete(dbMat.queue, k)
                      .put(dbMat.inflight, k.id, ())
                }
                .result
            }
            ret <- dbApi.transactionTask(tx)
          } yield ret

          val batchGetTask = for {
            keys <- UIO(batch.map(_._1.id))
            ret <- dbApi
              .columnFamily(dbMat.fact)
              .batchGetByKeysTask(keys)
          } yield ret.iterator.collect {
            case Some(pair) => pair
          }.toList

          val par = measure(deleteTask, metrics.purgeDuration) zipParRight measure(batchGetTask, metrics.getDuration)

          env.akkaService.unsafeRunToFuture(
            measure(par, metrics.duration)
              .provide(env)
          )
        }
    }
  }

  protected def run = {
    val populateMessages = new LongAdder()
    val populateBatches = new LongAdder()

    val dequeueMessages = new LongAdder()
    val responseMessages = new LongAdder()

    object purgeMetrics extends DequeueMetrics {
      val batches: LongAdder = new LongAdder()
      val cycles: LongAdder = new LongAdder()
      val emptyCycles: LongAdder = new LongAdder()

      val duration: LongAdder = new LongAdder()
      val purgeDuration: LongAdder = new LongAdder()
      val getDuration: LongAdder = new LongAdder()
    }

    object responseMetrics extends ResponseMetrics {
      val batches: LongAdder = new LongAdder()
      val duration: LongAdder = new LongAdder()
    }

    val parallelism = 2

    for {
      //      db <- RocksdbDatabase(
      //        dbMat,
      //        RocksdbDatabase.Config(
      //          path = "/Volumes/NKTPRO/Downloads/bench-db",
      //          readOnly = false,
      //          startWithBulkInserts = false,
      //          checksumOnRead = false,
      //          syncWriteBatch = true,
      //          useDirectIo = true,
      //          ioDispatcher = "dev.chopsticks.kvdb.db-io-dispatcher"
      //        )
      //      )
      db <- RocksdbDatabase(
        dbMat,
        RocksdbDatabase.Config(
          path = refineV[NonEmpty](sys.env("HOME") + "/Downloads/bench-db").right.get,
          readOnly = false,
          startWithBulkInserts = false,
          checksumOnRead = false,
          syncWriteBatch = false,
          useDirectIo = false,
          ioDispatcher = "dev.chopsticks.kvdb.db-io-dispatcher"
        )
      )
      api <- KvdbDatabaseApi(db)
      _ <- api.openTask().log("Open db")
      statsFib <- ZIO
        .access[LogEnv] {
          _.logger.info(
            s"populate=${populateMessages.longValue()} " +
              s"queue=${dequeueMessages.longValue()} " +
              s"res=${responseMessages.longValue()} " +
              s"populate-batch=${populateBatches.longValue()} " +
              s"queue-batch=${purgeMetrics.batches.longValue()} " +
              s"queue-repeat=${purgeMetrics.cycles.longValue()} " +
              s"queue-empty=${purgeMetrics.emptyCycles.longValue()} " +
              s"queue-ms=${purgeMetrics.duration.longValue()} " +
              s"queue-del-ms=${purgeMetrics.purgeDuration.longValue()} " +
              s"queue-get-ms=${purgeMetrics.getDuration.longValue()} " +
              s"res-batch=${responseMetrics.batches.longValue()} " +
              s"res-ms=${responseMetrics.duration.longValue()} "
          )
        }
        .repeat(ZSchedule.fixed(zio.duration.Duration(1, TimeUnit.SECONDS)))
        .fork

      populateSource <- createPopulateSource(
        dbApi = api,
        batchCounter = populateBatches,
        counter = populateMessages,
        parallelism = parallelism
      )

      dequeueFlow <- createDequeueFlow(
        dbApi = api,
        metrics = purgeMetrics,
        parallelism = parallelism
      )

      responseSource <- createResponseSource(
        dbApi = api,
        metrics = responseMetrics,
        parallelism = parallelism
      )

//      compactionFlow <- createCompactionFlow(
//        db = db
//      )

      populateThenPurgeFib <- ZAkkaStreams
        .interruptableGraph(
          ZIO.succeed {
            populateSource
              .via(dequeueFlow)
//              .wireTap(compactionFlow.to(Sink.foreach { duration =>
//                compactCounter.increment()
//                compactTimeCounter.add(duration.toMillis)
//              }))
              .mapConcat(identity)
//              .take(1000000)
//              .throttle(100000, 1.second)
              .viaMat(KillSwitches.single)(Keep.right)
              .toMat(Sink.foreach { _ =>
                dequeueMessages.increment()
              })(Keep.both)
          },
          graceful = true
        )
        .fork

      responseFib <- ZAkkaStreams
        .interruptableGraph(
          ZIO.succeed(
            responseSource
              .viaMat(KillSwitches.single)(Keep.right)
              .toMat(Sink.foreach { b =>
                responseMessages.add(b.size.toLong)
              })(Keep.both)
          ),
          graceful = true
        )
        .fork

      _ <- populateThenPurgeFib.join
      _ <- responseFib.join
      _ <- statsFib.join
    } yield ()
  }
}
