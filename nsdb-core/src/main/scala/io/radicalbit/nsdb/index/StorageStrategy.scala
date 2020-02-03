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

package io.radicalbit.nsdb.index

/**
  * Index Storage Strategies
  * Subclasses are
  *
  * - [[Memory]] Index is loaded in off-heap Memory (mmap)
  *
  * - [[FileSystem]] Index is loaded on the File system
  *
  * - [[Hybrid]] Index is loaded part in memory and on the File system
  *
  */
sealed trait StorageStrategy

object StorageStrategy {

  def withValue(storage: String): StorageStrategy = storage.toLowerCase match {
    case "hybrid"     => Hybrid
    case "memory"     => Memory
    case "filesystem" => FileSystem
    case _            => throw new IllegalArgumentException(s"unrecognized name for StorageStrategy $storage")
  }

  case object Hybrid     extends StorageStrategy
  case object Memory     extends StorageStrategy
  case object FileSystem extends StorageStrategy
}
