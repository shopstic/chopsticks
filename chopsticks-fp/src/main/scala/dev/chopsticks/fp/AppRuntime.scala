package dev.chopsticks.fp
import zio.{Unsafe, ZIO}

trait AppRuntime[R] extends zio.Runtime[R] { self =>

  /** The main function of the application, which will be passed the command-line arguments to the program and has to
    * return an `IO` with the errors fully handled.
    */
  def run(args: List[String]): ZIO[R, Nothing, Int]

  /** The Scala main function, intended to be called only by the Scala runtime.
    */
  // $COVERAGE-OFF$ Bootstrap to `Unit`
  final def main(args0: Array[String]): Unit =
    sys.exit(
      Unsafe.unsafe { implicit unsafe =>
        this.unsafe
          .run {
            for {
              fiber <- run(args0.toList).fork
              _ <- ZIO.succeed(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
                override def run(): Unit = {
                  val _ = self.unsafe.run(fiber.interrupt)
                }
              }))
              result <- fiber.join
            } yield result
          }
          .getOrThrowFiberFailure()
      }
    )
  // $COVERAGE-ON$
}
