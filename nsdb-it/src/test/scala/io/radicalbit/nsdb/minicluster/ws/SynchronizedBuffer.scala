package io.radicalbit.nsdb.minicluster.ws
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ListBuffer

/**
  * Provides utilities for managing a thread-safe buffer leveraging [[AtomicReference]] capabilities.
  * @tparam T Buffer element type.
  */
trait SynchronizedBuffer[T] {

  private[this] val _buffer = new AtomicReference(ListBuffer[T]())

  /**
    * Add an element to the buffer.
    * @param element the element to accumulate.
    */
  def accumulate(element: T): Unit = {
    update(_ += element)
  }

  /**
    * Removes an element from the buffer.
    * @param element the element to be removed.
    */
  def pop(element: T): Unit = {
    update(_ -= element)
  }

  /**
    * Remove all the elements from the buffer.
    */
  def clearBuffer(): Boolean = _buffer.compareAndSet(_buffer.get(), ListBuffer.empty)

  /**
    * @return the buffer state.
    */
  def buffer: ListBuffer[T] = _buffer.get()

  /**
    * Generic method to update the buffer given a transformation function
    * @param fn the transformation function to apply to the buffer
    */
  private[this] def update(fn: ListBuffer[T] => ListBuffer[T]): Unit = {
    while (true) {
      val listenerMap = _buffer.get()
      if (_buffer.compareAndSet(listenerMap, fn(listenerMap)))
        return // success
    }
  }
}
