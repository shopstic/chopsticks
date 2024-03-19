package dev.chopsticks.fdb.env

import com.apple.foundationdb.{Database, FDB, Transaction}
import dev.chopsticks.fp.iz_logging.IzLogging
import zio.{ZIO, ZLayer}

import scala.concurrent.Future
import scala.jdk.FutureConverters._
import dev.chopsticks.fp.zio_ext._

trait FdbEnv {
  def database: Database
  def runAsync[T](run: Transaction => Future[T]): Future[T]
}

object FdbEnv {
  final case class Live(database: Database) extends FdbEnv {
    def runAsync[T](run: Transaction => Future[T]): Future[T] = {
      database.runAsync { tx => run(tx).asJava.toCompletableFuture }.asScala
    }
  }

  def live(clusterFilePath: Option[String]): ZLayer[IzLogging, Nothing, FdbEnv] = {
    val effect =
      ZIO.acquireRelease {
        ZIO
          .blocking {
            ZIO
              .attempt {
                // TODO: this will no longer be needed once this PR makes it into a public release:
                // https://github.com/apple/foundationdb/pull/2635
                val m = classOf[FDB].getDeclaredMethod("selectAPIVersion", Integer.TYPE, java.lang.Boolean.TYPE)
                m.setAccessible(true)
                val fdb = m.invoke(null, 620, false).asInstanceOf[FDB]
                Live(clusterFilePath.fold(fdb.open)(fdb.open))
              }
              .orDie
          }
          .log("Open FDB database")
      } { service =>
        ZIO
          .blocking {
            ZIO
              .attempt {
                service.database.close()
                FDB.instance().stopNetwork()
              }
              .orDie
          }
          .log("Close FDB database")
      }

    ZLayer.scoped(effect)
  }
}
