package dev.chopsticks.sample.app

import dev.chopsticks.fp.iz_logging.LogCtx

object LoggingContextApp {
  def main(args: Array[String]): Unit = {
    test()
  }

  private def test()(implicit ctx: LogCtx): Unit = {
    println(ctx.sourceLocation)
  }
}
