package dev.chopsticks.fp.iz_logging

import izumi.logstage.api.Log
import izumi.logstage.api.rendering.RenderingOptions
import izumi.logstage.api.rendering.logunits.{LETree, Renderer, Styler}

object IzLoggingCustomStylers {
  def logLevelColor(lvl: Log.Level): String = lvl match {
    case Log.Level.Trace => Console.RESET
    case Log.Level.Debug => Console.RESET
    case Log.Level.Info => Console.RESET
    case Log.Level.Warn => Console.YELLOW
    case Log.Level.Error => Console.RED
    case Log.Level.Crit => Console.RED
  }

  final class LevelColor(sub: Seq[Renderer]) extends Styler {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree = {
      val color = logLevelColor(entry.context.dynamic.level)

      if (sub.tail.isEmpty) {
        LETree.ColoredNode(color, sub.head.render(entry, context))
      }
      else {
        LETree.ColoredNode(color, LETree.Sequence(sub.map(_.render(entry, context))))
      }
    }
  }

}
