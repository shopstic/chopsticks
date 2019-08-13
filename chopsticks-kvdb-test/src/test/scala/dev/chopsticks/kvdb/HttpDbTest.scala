package dev.chopsticks.kvdb

import java.util.concurrent.TimeUnit

import dev.chopsticks.kvdb.DbTest.DbTest
import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import org.scalatest.Assertion
import zio.{Task, RIO, UIO, ZIO}

import scala.concurrent.TimeoutException

class HttpDbTest extends DbTest {
  protected def runTest: (DbInterface[TestDb.type] => Task[Assertion]) => RIO[AkkaApp.Env, Assertion] = {
    (test: DbInterface[TestDb.type] => Task[Assertion]) =>
      {
        DbTest.withTempDir { dir =>
          for {
            dbServer <- ZIO.access[AkkaEnv] { implicit env =>
              val backendDb =
                LmdbDb[TestDb.type](
                  TestDb,
                  dir.pathAsString,
                  maxSize = 64 << 20,
                  noSync = false,
                  ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
                )
              HttpDbServer[TestDb.type](backendDb)
            }
            ret <- dbServer
              .start(0)
              .bracket(b => Task.fromFuture(_ => b.unbind()).catchAll(_ => UIO.unit)) { serverBinding =>
                import scala.concurrent.duration._

                ZIO
                  .access[AkkaEnv] { implicit env =>
                    HttpDb[TestDb.type](
                      TestDb,
                      host = "localhost",
                      port = serverBinding.localAddress.getPort,
                      keepAliveInterval = 30.seconds,
                      iterateFailureDelayIncrement = Duration.Zero,
                      iterateFailureDelayResetAfter = Duration.Zero,
                      iterateFailureMaxDelay = Duration.Zero
                    )
                  }
                  .bracket(_.closeTask().catchAll(_ => UIO.unit)) { dbClient =>
                    for {
                      _ <- dbClient.openTask()
                      ret <- test(dbClient)
                    } yield ret
                  }
              }
          } yield ret
        }
      }.timeoutFail(new TimeoutException("Test timed out"))(zio.duration.Duration(15, TimeUnit.SECONDS))
  }
}
