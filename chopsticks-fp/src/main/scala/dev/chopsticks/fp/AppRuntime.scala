package dev.chopsticks.fp
import zio.{IO, ZIO}

trait AppRuntime[R] extends zio.Runtime[R] {

  /**
    * The main function of the application, which will be passed the command-line
    * arguments to the program and has to return an `IO` with the errors fully handled.
    */
  def run(args: List[String]): ZIO[R, Nothing, Int]

  /**
    * The Scala main function, intended to be called only by the Scala runtime.
    */
  // $COVERAGE-OFF$ Bootstrap to `Unit`
  final def main(args0: Array[String]): Unit =
    sys.exit(
      unsafeRun(
        for {
          fiber <- run(args0.toList).fork
          _ <- IO.effectTotal(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
            override def run(): Unit = {
              val _ = unsafeRunSync(fiber.interrupt)
            }
          }))
          result <- fiber.join
        } yield result
      )
    )
  // $COVERAGE-ON$
}
