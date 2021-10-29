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

package io.radicalbit.nsdb.cluster.extension

import java.util.concurrent.atomic.AtomicReference

/**
  * Provides utilities for managing a thread-safe Map leveraging [[AtomicReference]] capabilities.
  *
  * @tparam K Map key type.
  * @tparam T Map value type.
  */
trait SynchronizedMap[K, T] {

  private[this] val _map = new AtomicReference(Map[K, T]())

  /**
    * Add an element to the buffer.
    * @param key the key to accumulate.
    * @param value the value to accumulate.
    */
  def accumulate(key: K, value: T): Unit = {
    update(_ + (key -> value))
  }

  /**
    * Removes an element from the buffer.
    * @param key the key to be removed.
    */
  def pop(key: K): Unit = {
    update(_ - key)
  }

  /**
    * Remove all the elements from the buffer.
    */
  def clear(): Unit = _map.set(Map.empty[K, T])

  /**
    * @return the buffer state.
    */
  protected def values: Iterable[T] = _map.get().values

  protected def get(key: K): Option[T] = _map.get().get(key)

  /**
    * Generic method to update the buffer given a transformation function
    * @param fn the transformation function to apply to the buffer
    */
  private[this] def update(fn: Map[K, T] => Map[K, T]): Unit = {
    while (true) {
      val listenerMap = _map.get()
      if (_map.compareAndSet(listenerMap, fn(listenerMap)))
        return // success
    }
  }
}
