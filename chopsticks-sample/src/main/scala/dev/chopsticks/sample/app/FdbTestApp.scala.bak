package dev.chopsticks.sample.app

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.LongAdder

import akka.stream.KillSwitches
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.apple.foundationdb.LocalityUtil
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import com.typesafe.config.Config
import dev.chopsticks.fdb.env.FdbEnv
import dev.chopsticks.fdb.util.FdbAsyncIteratorGraphStage
import dev.chopsticks.fp._
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.sample.util.{AppConfigLoader, MiscUtils}
import dev.chopsticks.stream.ZAkkaStreams
import eu.timepit.refined.types.numeric.PosInt
import pureconfig.ConfigReader
import zio.{blocking => _, _}

import scala.collection.immutable.ListMap
import scala.concurrent.{Future, TimeoutException}
import eu.timepit.refined.auto._

import scala.concurrent.duration._

object FdbTestApp extends AkkaApp {
  final case class AppConfig(
    clusterFilePath: Option[String],
    writesPerTx: PosInt,
    txParallelism: PosInt,
    subspaceCount: PosInt,
    keySize: PosInt,
    valueSize: PosInt
  )

  object AppConfig {
    //noinspection TypeAnnotation
    implicit val configConvert = {
      import dev.chopsticks.util.config.PureconfigConverters._
      ConfigReader[AppConfig]
    }
  }

  override type Env = AkkaApp.Env with FdbEnv with Has[AppConfig] with IzLogging

  override protected def createEnv(config: Config): ZLayer[AkkaApp.Env, Throwable, Env] = {
    val appConfig = AppConfigLoader.load[AppConfig](config)

    IzLogging.live(config) ++ ZLayer.requires[AkkaApp.Env] >+>
      (FdbEnv.live(appConfig.clusterFilePath) ++ ZLayer.succeed(appConfig))
  }

  private def createSubspaceSource(
    keySize: PosInt,
    valueSize: PosInt
  ): UIO[Source[(Array[Byte], Array[Byte]), NotUsed]] = {
    UIO {
      val subspace = new Subspace(Tuple.from(UUID.randomUUID()))
      val keyPadding = "0" * Math.max(0, keySize.value - subspace.getKey.length)
      val valuePadding = "0" * Math.max(0, valueSize.value - subspace.getKey.length)

      Source(1 to Int.MaxValue)
        .map { i =>
          val key = subspace.pack(Tuple.from(keyPadding, i))
          val value = subspace.pack(Tuple.from(valuePadding, i))
          key -> value
        }
    }
  }

  def sequentialWritesTask(
    source: Source[(Array[Byte], Array[Byte]), NotUsed],
    writesPerTx: PosInt,
    txParallelism: PosInt,
    txCounter: LongAdder
  ): RIO[AkkaEnv with LogEnv with FdbEnv, Done] = {
    val graphTask = for {
      akkaService <- ZService[AkkaEnv.Service]
      fdbService <- ZService[FdbEnv.Service]
    } yield {
      import akkaService.dispatcher

      source
        .grouped(writesPerTx)
        .mapAsyncUnordered(txParallelism) { group =>
          fdbService.runAsync { tx =>
            tx.options().setPriorityBatch()
            tx.options().setReadYourWritesDisable()
            Future {
              group.foreach {
                case (k, v) =>
                  tx.options().setNextWriteNoWriteConflictRange()
                  tx.set(k, v)
              }
            }
          }
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach(_ => txCounter.increment()))(Keep.both)
    }

    ZAkkaStreams.interruptibleGraphM(graphTask, graceful = true)
  }

  def fetchShardMap(
    beginKey: Array[Byte],
    endKey: Array[Byte]
  ): ZIO[AkkaEnv with LogEnv with FdbEnv, Throwable, Map[String, Int]] = {
    for {
      fdbService <- ZService[FdbEnv.Service]
      db = fdbService.database
      map <- ZAkkaStreams.graphM(
        ZService[AkkaEnv.Service].map(_.dispatcher).map { implicit ec =>
          Source
            .fromGraph(
              new FdbAsyncIteratorGraphStage(
                LocalityUtil.getBoundaryKeys(db, beginKey, endKey)
              )
            )
            .groupedWithin(1024, 1.milli)
            .mapAsync(1024) { keys =>
              import scala.jdk.FutureConverters._
              fdbService
                .runAsync(tx => {
                  tx.options().setIncludePortInAddress()
                  Future
                    .sequence(
                      keys.map(k => LocalityUtil.getAddressesForKey(tx, k).asScala)
                    )
                })
                .map { addresses => keys.zip(addresses) }
            }
            .mapConcat(identity)
            .toMat(Sink.fold(Map.empty[String, Int]) {
              case (map, (_, addresses)) =>
                addresses.foldLeft(map) { (m, address) => m.updated(address, m.getOrElse(address, 0) + 1) }
            })(Keep.right)
        }
      )
    } yield map
  }

  def fetchStatusJson: ZIO[FdbEnv, Throwable, String] = {
    for {
      db <- ZService[FdbEnv.Service].map(_.database)
      ret <- {
        import scala.jdk.FutureConverters._
        Task.fromFuture { _ =>
          val f: Future[Array[Byte]] = db.runAsync { tx =>
            tx.get(
              ByteArrayUtil
                .join(Array(0xFF.toByte, 0xFF.toByte), "/status/json".getBytes(StandardCharsets.US_ASCII))
            )
          }.asScala

          f
        }
      }
    } yield new String(ret, StandardCharsets.UTF_8)
  }

  override def run: RIO[Env, Unit] = {
    val app = for {
      _ <- fetchStatusJson
        .timeoutFail(new TimeoutException("Timed out fetching fdb status"))(2.seconds)
        .log("Fetch cluster status")
      appConfig <- ZService[AppConfig]
      writesPerTx = appConfig.writesPerTx
      txParallelism = appConfig.txParallelism
      txCounter = new LongAdder
      _ <- MiscUtils
        .logRates(1.second) {
          ListMap(
            "transactions" -> txCounter.doubleValue(),
            "keys" -> txCounter.doubleValue() * writesPerTx.value
          )
        }
        .fork
      _ <- ZIO
        .foreachPar_(1 to appConfig.subspaceCount.value) { i =>
          for {
            source <- createSubspaceSource(appConfig.keySize, appConfig.valueSize)
            _ <- sequentialWritesTask(source, writesPerTx, txParallelism, txCounter)
              .log(s"Benchmark $i")
          } yield ()
        }
        .log("All benchmarks")
    } yield ()

    app
  }
}
