package dev.chopsticks.fp.util

//object ZTraceConcisePrinter {
//  private def isZioInternal(element: ZTraceElement): Boolean = {
//    element match {
//      case loc: ZTraceElement.SourceLocation if loc.clazz.startsWith("zio.") => true
//      case _ => false
//    }
//  }
//
//  private def printElement(element: ZTraceElement): String = {
//    element match {
//      case loc: ZTraceElement.SourceLocation if loc.clazz.startsWith("zio.") => "<zio internals>"
//      case loc => loc.prettyPrint
//    }
//  }
//
//  private def printElements(trace: List[ZTraceElement], prefix: String): List[String] = {
//    trace.headOption match {
//      case Some(head) =>
//        val tail = if (trace.tail.nonEmpty) trace.sliding(2) else Iterator.empty
//
//        (Iterator.single(head :: Nil) ++ tail)
//          .foldLeft(List.newBuilder[String]) { (buf, pair) =>
//            pair match {
//              case prior :: next :: Nil =>
//                if (isZioInternal(prior) && isZioInternal(next)) {
//                  buf
//                }
//                else {
//                  buf += prefix + printElement(next)
//                }
//              case next :: Nil =>
//                buf += prefix + printElement(next)
//              case _ => buf
//            }
//          }
//          .result()
//
//      case None => Nil
//    }
//  }
//
//  def prettyPrint(trace: ZTrace): String = {
//    val executionTrace = trace.executionTrace
//    val stackTrace = trace.stackTrace
//    val fiberId = trace.fiberId
//
//    val stackPrint = printElements(stackTrace, "  a future continuation at ") match {
//      case Nil =>
//        s"Fiber:$fiberId was supposed to continue to: <empty trace>" :: Nil
//      case lines =>
//        s"Fiber:$fiberId was supposed to continue to:" :: lines
//    }
//
//    val execPrint = printElements(executionTrace, "  at ") match {
//      case Nil =>
//        s"Fiber:$fiberId execution trace: <empty trace>" :: Nil
//      case lines =>
//        s"Fiber:$fiberId execution trace:" :: lines
//    }
//
//    val ancestry: List[String] =
//      trace
//        .parentTrace
//        .map(parent => s"Fiber:$fiberId was spawned by:\n" :: ZTraceConcisePrinter.prettyPrint(parent) :: Nil)
//        .getOrElse(s"Fiber:$fiberId was spawned by: <empty trace>" :: Nil)
//
//    (stackPrint ++ ("" :: execPrint) ++ ("" :: ancestry)).mkString("\n")
//  }
//}
