package dev.chopsticks.fdb.lease

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import dev.chopsticks.fdb.lease.LeaseSupervisor.LeaseSupervisorConfig
import dev.chopsticks.fdb.lease.LeaseSupervisorTest.LeaseSupervisorTestDatabase.LeaseSupervisorTestDbCf
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.LiveDiEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.iz_logging.IzLogging.IzLoggingConfig
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers, LoggingContext}
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.api.KvdbDatabaseApi
import dev.chopsticks.kvdb.codec.{KeySerdes, ValueSerdes}
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDefinition, KvdbMaterialization}
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbDatabaseConfig
import dev.chopsticks.kvdb.fdb.{FdbDatabase, FdbMaterialization}
import dev.chopsticks.kvdb.util.KvdbIoThreadPool
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import eu.timepit.refined.auto._
import izumi.distage.constructors.HasConstructor
import logstage.Log
import org.scalatest.{Inspectors, Succeeded}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Has, RIO, RLayer, Task, UIO, URLayer, ZIO, ZLayer, ZManaged}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object LeaseSupervisorTest {
  import dev.chopsticks.kvdb.codec.fdb_key._

  implicit lazy val leaseKeySerdes = KeySerdes[LeaseKey]
  implicit lazy val leaseSerdes = ValueSerdes.fromKeySerdes(KeySerdes[Lease])

  final private[lease] case class LeaseSupervisorTestConfig(
    fdbConfig: FdbDatabaseConfig,
    leaseSupervisor: LeaseSupervisorConfig
  )

  private[lease] object LeaseSupervisorTestDatabase extends KvdbDefinition {
    type LeaseSupervisorTestDbCf[K, V] = BaseCf[K, V]
    type Fdb = FdbDatabase[LeaseSupervisorTestDbCf, CfSet]

    trait LeaseKeyspace extends LeaseSupervisorTestDbCf[LeaseKey, Lease]

    type CfSet = LeaseKeyspace

    trait Materialization extends KvdbMaterialization[BaseCf, CfSet] with FdbMaterialization[BaseCf] {
      def lease: LeaseKeyspace

      lazy val columnFamilySet: ColumnFamilySet[LeaseSupervisorTestDbCf, LeaseKeyspace] =
        ColumnFamilySet[LeaseSupervisorTestDatabase.BaseCf] of lease

      override def keyspacesWithVersionstampKey = Set.empty
      override def keyspacesWithVersionstampValue = Set.empty
    }
  }

  private[lease] object LeaseFdbMaterialization extends LeaseSupervisorTestDatabase.Materialization {
    object lease extends LeaseSupervisorTestDatabase.LeaseKeyspace
  }

  private[lease] val liveDb
    : RLayer[AkkaEnv with Blocking with MeasuredLogging with KvdbIoThreadPool, Has[LeaseSupervisorTestDatabase.Db]] = {
    val materialization: LeaseSupervisorTestDatabase.Materialization = LeaseFdbMaterialization
    val managed = for {
      db <- FdbDatabase.manage(
        materialization,
        FdbDatabase.FdbDatabaseConfig(
          clusterFilePath = Some(sys.env("HOME") + "/.fdb/cluster.file"),
          rootDirectoryPath = UUID.randomUUID().toString,
          stopNetworkOnClose = false
        )
      )
      _ <- ZManaged.make(ZIO.succeed(db)) { database =>
        ZIO.foreachPar_(LeaseFdbMaterialization.columnFamilySet.value) { column =>
          database.dropColumnFamily(column).orDie
        }
      }
    } yield db
    managed.toLayer
  }

  private[lease] val liveApi: ZLayer[
    AkkaEnv with Has[LeaseSupervisorTestDatabase.Db],
    Nothing,
    Has[KvdbDatabaseApi[LeaseSupervisorTestDatabase.BaseCf]]
  ] = {
    val managed = for {
      db <- ZManaged.access[Has[LeaseSupervisorTestDatabase.Db]](_.get)
      api <- KvdbDatabaseApi(db).toManaged_
    } yield api
    managed.toLayer
  }

  private[lease] def liveSupervisor(config: LeaseSupervisorConfig)
    : RLayer[AkkaEnv with MeasuredLogging with Has[KvdbDatabaseApi[LeaseSupervisorTestDbCf]] with Has[
      LeaseSupervisorTestDatabase.Db
    ], Has[LeaseSupervisor.Service[LeaseSupervisorTestDatabase.LeaseKeyspace]]] = {
    val managed = for {
      db <- ZManaged.access[Has[LeaseSupervisorTestDatabase.Db]](_.get)
      fdb <- {
        db match {
          case fdb: FdbDatabase[LeaseSupervisorTestDatabase.LeaseSupervisorTestDbCf, _] =>
            ZManaged.succeed(fdb)
          case _ =>
            ZManaged.fail(new IllegalArgumentException(s"FDB database is required, got $db instead"))
        }
      }
      leaseSupervisor <- {
        val leaseKeyspace: LeaseSupervisorTestDatabase.LeaseKeyspace = LeaseFdbMaterialization.lease
        LeaseSupervisor
          .live[LeaseSupervisorTestDatabase.LeaseKeyspace, LeaseSupervisorTestDatabase.LeaseSupervisorTestDbCf](
            config,
            fdb,
            leaseKeyspace
          )
          .build.map(_.get)
      }
    } yield leaseSupervisor
    managed.toLayer
  }

}

class LeaseSupervisorTest
    extends AkkaTestKit
    with AsyncWordSpecLike
    with Matchers
    with Inspectors
    with AkkaTestKitAutoShutDown
    with LoggingContext {
  import LeaseSupervisorTest._

  private val runtime = AkkaDiApp.createRuntime(AkkaEnv.live(system) ++ IzLogging.live(typesafeConfig))
  private val akkaAppDi = AkkaDiApp.Env.createModule(system)
  private val shutdown = CoordinatedShutdown(system)

  "LeaseSupervisor" should {
    "acquire all leases if none exists" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        config <- useConfig
        leasesChange <- Task.fromFuture(_ => leaseSupervisor.acquireLeases.runWith(Sink.head))
        _ <- Task {
          val obtainedPartitions = leasesChange.newSessions.map(_.lease.partitionNumber)
          val expectedPartitions = (0 until config.leaseSupervisor.partitionCount)
          obtainedPartitions should contain theSameElementsAs expectedPartitions
        }
        _ <- Task(leasesChange.snapshot should contain theSameElementsAs leasesChange.newSessions)
      } yield ()
    }

    "acquire all leases if none of the exiting leases is valid" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        config <- useConfig
        acquirer <- useLeaseAcquirer
        _ <- {
          ZIO.foreach((0 until config.leaseSupervisor.partitionCount).toList) { partitionNumber =>
            val expirationDurationMs = config.leaseSupervisor.expirationDuration.toMillis
            acquirer.forceAcquire(
              partitionNumber,
              lease => lease.copy(refreshedAt = lease.refreshedAt.minusMillis(expirationDurationMs + 1))
            )
          }
        }
        leasesChange <- Task.fromFuture(_ => leaseSupervisor.acquireLeases.runWith(Sink.head))
        _ <- Task {
          val obtainedPartitions = leasesChange.newSessions.map(_.lease.partitionNumber)
          val expectedPartitions = (0 until config.leaseSupervisor.partitionCount)
          obtainedPartitions should contain theSameElementsAs expectedPartitions
        }
        _ <- Task(leasesChange.snapshot should contain theSameElementsAs leasesChange.newSessions)
      } yield ()
    }

    "only not acquire any leases if all existing leases are valid" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        config <- useConfig
        acquirer <- useLeaseAcquirer
        _ <- {
          ZIO.foreach((0 until config.leaseSupervisor.partitionCount).toList) { partitionNumber =>
            acquirer.forceAcquire(partitionNumber)
          }
        }
        leasesChange <- Task.fromFuture(_ => leaseSupervisor.acquireLeases.runWith(Sink.head))
        _ <- Task(leasesChange.newSessions shouldBe empty)
        _ <- Task(leasesChange.snapshot shouldBe empty)
      } yield ()
    }

    "refresh its leases" in leaseSupervisorTest() {
      (for {
        leaseSupervisor <- useLeaseSupervisor.toManaged_
        probe <-
          ZManaged.make(Task(leaseSupervisor.acquireLeases.runWith(TestSink.probe)))(probe => UIO(probe.cancel()).unit)
      } yield probe).use { probe =>
        for {
          config <- useConfig
          acquirer <- useLeaseAcquirer
          initialChange <- Task(probe.requestNext())
          initialFromDb <- acquirer.getAllLeases()
          _ <- Task(initialChange.snapshot should have size config.leaseSupervisor.partitionCount.value.toLong)
          _ <- Task(initialChange.snapshot.map(_.lease) should contain theSameElementsAs initialFromDb)

          subsequentChange <- Task(probe.requestNext())
          subsequentFromDb <- acquirer.getAllLeases()
          _ <- Task {
            val zipped = initialFromDb.sortBy(_.partitionNumber).zip(subsequentFromDb.sortBy(_.partitionNumber))
            forAll(zipped) {
              case (initial, subsequent) =>
                assert(initial.refreshedAt.isBefore(subsequent.refreshedAt))
                assert(initial.copy(refreshedAt = subsequent.refreshedAt) === subsequent)
            }
          }
          _ <- Task(subsequentChange.snapshot.map(_.lease) should contain theSameElementsAs subsequentFromDb)
        } yield ()
      }
    }

    "remove its leases if other replica is waiting for them" in leaseSupervisorTest() {
      (for {
        leaseSupervisor <- useLeaseSupervisor.toManaged_
        probe <-
          ZManaged.make(Task(leaseSupervisor.acquireLeases.runWith(TestSink.probe)))(probe => UIO(probe.cancel()).unit)
      } yield probe).use { probe =>
        for {
          acquirer <- useLeaseAcquirer
          clock <- useClock

          _ <- Task(probe.requestNext())
          initialFromDb <- acquirer.getAllLeases()
          now <- clock.currentDateTime.map(_.toInstant)

          initialFirstPartitionLease = initialFromDb.minBy(_.partitionNumber)
          modifiedFirstPartitionLease = initialFirstPartitionLease.copy(assigneeWaitingFrom = Some(now))
          _ <- acquirer.setLease(modifiedFirstPartitionLease)

          subsequentChange <- Task(probe.requestNext())
          subsequentFromDb <- acquirer.getAllLeases()
          _ <- Task {
            val subsequentFirstPartitionLease = subsequentChange.snapshot.minBy(_.lease.partitionNumber)
            assert(initialFirstPartitionLease.partitionNumber < subsequentFirstPartitionLease.lease.partitionNumber)
            assert(subsequentFromDb.minBy(_.partitionNumber) === modifiedFirstPartitionLease.copy(owner = None))
          }
        } yield ()
      }
    }

    "signal that its waiting for its own partitions and it should acquire them once they are freed" in leaseSupervisorTest() {
      (for {
        leaseSupervisor <- useLeaseSupervisor.toManaged_
        probe <-
          ZManaged.make(Task(leaseSupervisor.acquireLeases.runWith(TestSink.probe)))(probe => UIO(probe.cancel()).unit)
      } yield probe).use { probe =>
        for {
          config <- useConfig
          acquirer <- useLeaseAcquirer
          _ <- {
            ZIO.foreach((0 until config.leaseSupervisor.partitionCount).toList) { partitionNumber =>
              acquirer.forceAcquire(partitionNumber)
            }
          }
          _ <- Task(probe.requestNext())

          allLeases <- acquirer.getAllLeases()
          _ <- {
            ZIO.foreach(allLeases.filter(_.assigneeWaitingFrom.nonEmpty)) { lease =>
              acquirer.setLease(lease.copy(owner = None))
            }
          }

          subsequentChange <- Task(probe.requestNext())
          _ <- Task {
            val obtainedPartitions = subsequentChange.snapshot.map(_.lease.partitionNumber)
            obtainedPartitions should contain theSameElementsAs config.leaseSupervisor.ownPartitions.toList
            subsequentChange.snapshot should contain theSameElementsAs subsequentChange.newSessions
          }
        } yield ()
      }
    }

    "delete its leases and remove information from leases it's waiting on when it terminates" in leaseSupervisorTest() {
      for {
        config <- useConfig
        acquirer <- useLeaseAcquirer
        ownPartitions = config.leaseSupervisor.ownPartitions
        _ <- {
          ZIO.foreach((2 until config.leaseSupervisor.partitionCount).toList) { partitionNumber =>
            acquirer.forceAcquire(partitionNumber)
          }
        }
        env <- ZIO.environment[AkkaEnv with MeasuredLogging with Has[
          KvdbDatabaseApi[LeaseSupervisorTestDbCf]
        ] with Has[LeaseSupervisorTestDatabase.Db]]
        _ <- liveSupervisor(config.leaseSupervisor).build.provide(env).use {
          hasSupervisor =>
            val supervisor = hasSupervisor.get
            for {
              change <- Task.fromFuture(_ => supervisor.acquireLeases.runWith(Sink.head))
              _ <- Task(change.snapshot.map(_.lease.partitionNumber) should contain theSameElementsAs List(0, 1))
              allLeases <- acquirer.getAllLeases()
              _ <- Task {
                forAll(allLeases.filter(l => ownPartitions.contains(l.partitionNumber))) { lease =>
                  assert(lease.assigneeWaitingFrom.isDefined)
                }
              }
            } yield ()
        }
        allLeases <- acquirer.getAllLeases()
        _ <- Task {
          forAll(allLeases.filter(l => ownPartitions.contains(l.partitionNumber))) { lease =>
            assert(lease.assigneeWaitingFrom.isEmpty)
          }
          forAll(allLeases.filter(l => Set(0, 1).contains(l.partitionNumber))) { lease =>
            assert(lease.owner.isEmpty)
          }
        }
      } yield ()
    }

    "successfully transact if provided lease is valid when transaction takes place" in leaseSupervisorTest() {
      for {
        config <- useConfig
        leaseSupervisor <- useLeaseSupervisor
        acquirer <- useLeaseAcquirer
        dbApi <- useDbApi
        clock <- useClock
        lease <- acquirer.forceAcquire(0, _.copy(owner = Some(config.leaseSupervisor.replicaIndex)))
        now <- clock.currentDateTime.map(_.toInstant)
        newLease <- {
          val newLease = Lease(
            partitionNumber = 1,
            owner = None,
            refreshedAt = now,
            assigneeWaitingFrom = None,
            acquisitionId = UUID.randomUUID()
          )
          val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
          leaseSupervisor.transact(lease, writes.result).as(newLease)
        }
        allLeases <- acquirer.getAllLeases()
        _ <- Task(List(lease, newLease) should contain theSameElementsAs allLeases)
      } yield ()
    }

    "fail transaction if provided lease belongs to different replica" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        acquirer <- useLeaseAcquirer
        dbApi <- useDbApi
        clock <- useClock
        lease <- acquirer.forceAcquire(0, _.copy(owner = Some(Int.MaxValue)))
        now <- clock.currentDateTime.map(_.toInstant)
        result <- {
          val newLease = Lease(
            partitionNumber = 1,
            owner = None,
            refreshedAt = now,
            assigneeWaitingFrom = None,
            acquisitionId = UUID.randomUUID()
          )
          val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
          leaseSupervisor.transact(lease, writes.result).as(newLease).either
        }
        _ <- Task(assert(result.isLeft))
      } yield ()
    }

    "fail transaction if provided lease doesn't belong to any replica" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        acquirer <- useLeaseAcquirer
        clock <- useClock
        dbApi <- useDbApi
        lease <- acquirer.forceAcquire(0, _.copy(owner = None))
        now <- clock.currentDateTime.map(_.toInstant)
        result <- {
          val newLease = Lease(
            partitionNumber = 1,
            owner = None,
            refreshedAt = now,
            assigneeWaitingFrom = None,
            acquisitionId = UUID.randomUUID()
          )
          val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
          leaseSupervisor.transact(lease, writes.result).as(newLease).either
        }
        _ <- Task(assert(result.isLeft))
      } yield ()
    }

    "fail if provided lease contains different acquisitionId than lease from DB" in leaseSupervisorTest() {
      for {
        config <- useConfig
        leaseSupervisor <- useLeaseSupervisor
        acquirer <- useLeaseAcquirer
        clock <- useClock
        dbApi <- useDbApi
        lease <- acquirer.forceAcquire(0, _.copy(owner = Some(config.leaseSupervisor.replicaIndex)))
        now <- clock.currentDateTime.map(_.toInstant)
        result <- {
          val newLease = Lease(
            partitionNumber = 1,
            owner = None,
            refreshedAt = now,
            assigneeWaitingFrom = None,
            acquisitionId = UUID.randomUUID()
          )
          val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
          leaseSupervisor.transact(lease.copy(acquisitionId = UUID.randomUUID()), writes.result).as(newLease).either
        }
        _ <- Task(assert(result.isLeft))
      } yield ()
    }

    "signal that session has ended when lease expires" in leaseSupervisorTest() {
      for {
        leaseSupervisor <- useLeaseSupervisor
        acquirer <- useLeaseAcquirer
        clock <- useClock

        initialSession <- {
          Task
            .fromFuture(_ => leaseSupervisor.acquireLeases.runWith(Sink.head))
            .map(_.snapshot.minBy(_.lease.partitionNumber))
        }

        _ <- acquirer.forceAcquire(0, _.copy(owner = Some(Int.MaxValue)))
        dbApi <- useDbApi
        now <- clock.currentDateTime.map(_.toInstant)
        result <- {
          val newLease = Lease(
            partitionNumber = 1,
            owner = None,
            refreshedAt = now,
            assigneeWaitingFrom = None,
            acquisitionId = UUID.randomUUID()
          )
          val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
          leaseSupervisor.transact(initialSession.lease, writes.result).as(newLease).either
        }
        _ <- Task(assert(result.isLeft))
        _ <- initialSession.expirationSignal.timeout(zio.duration.Duration(5, TimeUnit.SECONDS))
      } yield ()
    }

  }

  trait LeaseAcquirer {
    def getAllLeases(): Task[Seq[Lease]]
    def forceAcquire(partitionNumber: Int): Task[Lease] = forceAcquire(partitionNumber, identity)
    def forceAcquire(partitionNumber: Int, customizeLease: Lease => Lease): Task[Lease]
    def setLease(newLease: Lease): Task[Lease]
  }

  object LeaseAcquirer {
    def live: URLayer[Clock with Has[KvdbDatabaseApi[LeaseSupervisorTestDbCf]], Has[LeaseAcquirer]] = {
      val managed =
        for {
          dbApi <- ZManaged.access[Has[KvdbDatabaseApi[LeaseSupervisorTestDbCf]]](_.get)
          clock <- ZManaged.access[Clock](_.get)
        } yield new LeaseAcquirer {
          override def getAllLeases(): Task[Seq[Lease]] = {
            dbApi
              .columnFamily(LeaseFdbMaterialization.lease)
              .getRangeTask(_.first, _.last, 10000)
              .map { xs =>
                xs.map { case (_, v) => v }
              }
          }

          override def forceAcquire(partitionNumber: Int, customizeLease: Lease => Lease): Task[Lease] = {
            for {
              now <- clock.currentDateTime.map(_.toInstant)
              lease <- {
                val l = Lease(
                  partitionNumber = partitionNumber,
                  owner = Some(Int.MaxValue),
                  refreshedAt = now,
                  assigneeWaitingFrom = None,
                  acquisitionId = UUID.randomUUID()
                )
                setLease(customizeLease(l))
              }
            } yield lease
          }

          override def setLease(newLease: Lease) = {
            val writes = dbApi.transactionBuilder.put(LeaseFdbMaterialization.lease, LeaseKey(newLease), newLease)
            dbApi.transact(writes.result).as(newLease)
          }
        }
      managed.toLayer
    }
  }

  private def useDbApi = {
    ZIO.service[KvdbDatabaseApi[LeaseSupervisorTestDbCf]]
  }

  private def useLeaseSupervisor = {
    ZIO.service[LeaseSupervisor.Service[LeaseSupervisorTestDatabase.LeaseKeyspace]]
  }

  private def useConfig = {
    ZIO.service[LeaseSupervisorTestConfig]
  }

  private def useLeaseAcquirer = {
    ZIO.service[LeaseAcquirer]
  }

  private def useClock = {
    ZIO.access[Clock](_.get)
  }

  private def defaultConfig = {
    val fdbConfig = {
      val rootDirectoryPath = Random.alphanumeric.take(10).mkString("")
      val clusterFilePath = Some(sys.env("HOME") + "/.fdb/cluster.file")
      FdbDatabaseConfig(
        clusterFilePath = clusterFilePath,
        rootDirectoryPath = rootDirectoryPath,
        stopNetworkOnClose = false,
        clientOptions =
          KvdbClientOptions().copy(disableWriteConflictChecking = true, tailPollingMaxInterval = Duration.Zero)
      )
    }
    LeaseSupervisorTestConfig(
      fdbConfig = fdbConfig,
      leaseSupervisor =
        LeaseSupervisorConfig(
          partitionCount = 10,
          replicaCount = 3,
          replicaIndex = 1,
          refreshInterval = 500.millis,
          expirationDuration = 1200.millis
        )
    )
  }

  private def leaseSupervisorTest[R <: Has[_]: HasConstructor](
    customizeConfig: LeaseSupervisorTestConfig => LeaseSupervisorTestConfig = identity
  )(test: => RIO[R, Unit]): Future[Succeeded.type] = {
    val config = customizeConfig(defaultConfig)
    val runEnvIo: UIO[DiEnv[AppEnv]] = UIO {
      LiveDiEnv(
        DiLayers(
          IzLogging.live(IzLoggingConfig(Log.Level.Warn, coloredOutput = true, None)),
          ZLayer.succeed(config),
          LeaseAcquirer.live,
          KvdbIoThreadPool.live(keepAliveTimeMs = 5000),
          liveDb,
          liveApi,
          liveSupervisor(config.leaseSupervisor),
          AppLayer(test),
          ZIO.environment[AppEnv]
        ) ++ akkaAppDi
      )
    }

    val task = for {
      runEnv <- runEnvIo
      exitCode <- runEnv.run(ZIO.accessM[AppEnv](_.get))
    } yield {
      if (exitCode == 0) Succeeded
      else throw new RuntimeException(s"Test failed with non-zero exit code of: $exitCode")
    }

    runtime.unsafeRunToFuture {
      ZManaged
        .makeInterruptible {
          task.forkDaemon.map { fib =>
            val cancelShutdownHook = shutdown.addCancellableTask("app-interruption", s"interrupt test") { () =>
              runtime.unsafeRunToFuture(fib.interrupt.ignore *> UIO(Done))
            }
            fib -> cancelShutdownHook
          }
        } {
          case (_, cancelShutdownHook) =>
            UIO {
              val _ = cancelShutdownHook.cancel()
            }
        }
        .map(_._1)
        .use(_.join)
    }

  }

}
