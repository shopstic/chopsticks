package dev.chopsticks.fdb

import com.apple.foundationdb.{Database, FDB, Transaction}
import dev.chopsticks.fp.LoggingContext
import zio.blocking.{blocking, Blocking}
import zio.{Has, Task, ZLayer, ZManaged}

import scala.concurrent.Future
import scala.jdk.FutureConverters._
import dev.chopsticks.fp.zio_ext._

package object env {
  type FdbEnv = Has[FdbEnv.Service]

  object FdbEnv extends LoggingContext {
    trait Service {
      def database: Database
      def runAsync[T](run: Transaction => Future[T]): Future[T]
    }

    final case class Live(database: Database) extends Service {
      def runAsync[T](run: Transaction => Future[T]): Future[T] = {
        database.runAsync { tx => run(tx).asJava.toCompletableFuture }.asScala
      }
    }

    def live(clusterFilePath: Option[String]): ZLayer[Blocking with MeasuredLogging, Nothing, FdbEnv] = {
      ZLayer.fromManaged {
        ZManaged.make {
          blocking {
            Task {
              // TODO: this will no longer be needed once this PR makes it into a public release:
              // https://github.com/apple/foundationdb/pull/2635
              val m = classOf[FDB].getDeclaredMethod("selectAPIVersion", Integer.TYPE, java.lang.Boolean.TYPE)
              m.setAccessible(true)
              val fdb = m.invoke(null, 620, false).asInstanceOf[FDB]
              Live(clusterFilePath.fold(fdb.open)(fdb.open))
            }.orDie
          }.log("Open FDB database")
        } { service =>
          blocking {
            Task {
              service.database.close()
              FDB.instance().stopNetwork()
            }.orDie
          }.log("Close FDB database")
        }
      }
    }
  }
}
