package dev.chopsticks.fp.iz_logging

import izumi.fundamentals.platform.language.Quirks
import izumi.logstage.api.Log
import izumi.logstage.api.rendering.RenderingOptions
import izumi.logstage.api.rendering.logunits.{Extractor, LETree, Renderer}

object IzLoggingCustomRenderers {
  val LocationCtxKey = "location"
  val LoggerTypeCtxKey = "loggerType"
  private val ExcludedCtxKeys = Set(LocationCtxKey, LoggerTypeCtxKey)

  final class ContextSourcePositionExtractor(fallback: Extractor) extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree.TextNode = {
      entry.context.customContext.values.find(_.path.exists(_ == LocationCtxKey)) match {
        case Some(arg) =>
          val stringArg = if (arg.value == null) "null" else "(" + arg.value.toString + ")"
          LETree.TextNode(stringArg)
        case None =>
          fallback.render(entry, context)
      }
    }
  }

  final class FilteringContextExtractor(fallback: Extractor) extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree.TextNode = {
      val originalCtx = entry.context.customContext
      val filteredCustomContext = {
        val filteredValues = originalCtx.values.filterNot(_.path.exists(ExcludedCtxKeys))
        originalCtx.copy(values = filteredValues)
      }
      val filteredEntry = entry.copy(context = entry.context.copy(customContext = filteredCustomContext))
      fallback.render(filteredEntry, context)
    }
  }

  object LoggerName extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree.TextNode = {
      Quirks.discard(context)
      LETree.TextNode("(" + entry.context.static.id.id + ")")
    }
  }

  final class LocationRenderer(sourceExtractor: Extractor, fallbackRenderer: Renderer) extends Renderer {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree = {
      entry.context.customContext.values.find(_.path.exists(_ == LoggerTypeCtxKey)) match {
        case Some(_) =>
          sourceExtractor.render(entry, context)
        case None =>
          fallbackRenderer.render(entry, context)
      }
    }
  }

  final class LoggerContext extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree.TextNode = {
      val values = entry.context.customContext.values
      val out =
        if (values.nonEmpty) {
          values
            .map {
              v => IzLoggingCustomLogFormat.formatKv(context.colored)(v.name, v.codec, v.value)
            }
            .mkString(", ")
        }
        else {
          ""
        }

      LETree.TextNode(out)
    }
  }

  final class MessageExtractor extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree.TextNode = {
      LETree.TextNode(IzLoggingCustomLogFormat.formatMessage(entry, context).message)
    }
  }

  final class ConcatRenderer(renderers: Seq[Renderer]) extends Renderer {
    override def render(entry: Log.Entry, context: RenderingOptions): LETree = {
      LETree.Sequence(renderers.map(_.render(entry, context)))
    }
  }
}
