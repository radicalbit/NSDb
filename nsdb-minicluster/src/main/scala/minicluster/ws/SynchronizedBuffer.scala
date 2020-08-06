/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  def clearBuffer(): Unit = _buffer.set(ListBuffer.empty)

  /**
    * @return the buffer state.
    */
  protected def buffer: ListBuffer[T] = _buffer.get()

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
