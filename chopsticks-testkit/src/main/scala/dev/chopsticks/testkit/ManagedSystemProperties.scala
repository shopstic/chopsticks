package dev.chopsticks.testkit

import com.typesafe.config.ConfigFactory
import zio.ZIO

trait ManagedSystemProperties {
  def manageSystemProperties[R, E, A](props: Map[String, String])(use: => ZIO[R, E, A]): ZIO[R, E, A] = {
    ZIO.scoped[R] {
      ZIO
        .acquireRelease {
          ZIO.succeed {
            val _ = sys.props ++= props
            ConfigFactory.invalidateCaches()
          }
        } { _ =>
          ZIO.succeed {
            props.keys.foreach(sys.props.remove)
            ConfigFactory.invalidateCaches()
          }
        }
        .zipRight(use)
    }
  }
}
