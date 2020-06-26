package dev.chopsticks.kvdb.util

import java.util.concurrent.atomic.AtomicReference

import akka.{Done, NotUsed}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.Try

final class KvdbCloseSignal {
  final class Listener private[KvdbCloseSignal] {
    private[KvdbCloseSignal] val promise = Promise[Done]()
    def future: Future[Done] = promise.future
    def unregister(): Unit = removeListener(this)
  }

  private[this] val _listeners = TrieMap.empty[Listener, NotUsed]
  private[this] val _completedWith: AtomicReference[Option[Try[Done]]] = new AtomicReference(None)

  def tryComplete(result: Try[Done]): Unit = {
    if (_completedWith.compareAndSet(None, Some(result))) {
      for ((listener, _) <- _listeners) listener.promise.tryComplete(result)
    }
  }

  def createListener(): Listener = {
    val listener = new Listener
    if (_completedWith.get.isEmpty) {
      val _ = _listeners += (listener -> NotUsed)
    }
    _completedWith.get match {
      case Some(result) => val _ = listener.promise.tryComplete(result)
      case None => // Ignore.
    }
    listener
  }

  def hasNoListeners: Boolean = _listeners.isEmpty

  private def removeListener(listener: Listener): Unit = {
    val _ = _listeners -= listener
  }
}
