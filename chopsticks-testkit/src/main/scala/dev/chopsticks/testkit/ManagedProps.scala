package dev.chopsticks.testkit

import com.typesafe.config.ConfigFactory
import zio.{UIO, ZIO, ZManaged}

trait ManagedProps {
  def manageProps[R, E, A](props: Map[String, String])(use: => ZIO[R, E, A]): ZIO[R, E, A] = {
    ZManaged
      .make {
        UIO {
          val _ = sys.props ++= props
          ConfigFactory.invalidateCaches()
        }
      } { _ =>
        UIO {
          props.keys.foreach(sys.props.remove)
          ConfigFactory.invalidateCaches()
        }
      }.use(_ => use)
  }
}
