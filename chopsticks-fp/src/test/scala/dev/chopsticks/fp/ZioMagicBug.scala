package dev.chopsticks.fp
import zio.magic._
import zio.{ExitCode, URIO, ZEnv, ZIO, ZLayer}

object ZioMagicBug extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val effect = for {
      intNumber <- ZIO.service[Int]
      doubleNumber <- ZIO.service[Double]
      longNumber <- ZIO.service[Long]
      _ <- zio.console.putStrLn(s"Int is $intNumber, Double is $doubleNumber and Long is $longNumber")
    } yield ()

    val layer1 = ZIO.service[Int].map(_.toDouble).toLayer
    val layer2 = ZIO.service[Int].map(_.toLong).toLayer

    effect
      .as(ExitCode(0))
      .provideSomeMagicLayer[ZEnv](
        layer1,
        layer2, {
          println("This will be evaluated 3 times")
          ZLayer.succeed(123)
        }
      )
  }
}
