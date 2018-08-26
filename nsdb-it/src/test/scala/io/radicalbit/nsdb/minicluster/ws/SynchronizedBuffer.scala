package io.radicalbit.nsdb.minicluster.ws
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ListBuffer

trait SynchronizedBuffer[T] {

  private[this] val buffer = new AtomicReference(ListBuffer[T]())

  def accumulate(fn: T): Unit = {
    update(_ += fn)
  }

  def pop(ref: T): Unit = {
    update(_ -= ref)
  }

  def clearBuffer() = buffer.compareAndSet(buffer.get(), ListBuffer.empty)

  def getBuffer() = buffer.get()

  private[this] def update(fn: ListBuffer[T] => ListBuffer[T]): Unit = {
    while (true) {
      val listenerMap = buffer.get()
      if (buffer.compareAndSet(listenerMap, fn(listenerMap)))
        return // success
    }
  }
}
