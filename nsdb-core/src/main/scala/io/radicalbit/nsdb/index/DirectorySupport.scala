/*
 * Copyright 2018 Radicalbit S.r.l.
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

import java.nio.file.Path

import org.apache.lucene.store.{FileSwitchDirectory, MMapDirectory, NIOFSDirectory}

import scala.collection.JavaConverters._

/**
  * Trait containing common Lucene [[org.apache.lucene.store.Directory]] custom factory methods.
  */
trait DirectorySupport {

  /**
    * extensions for norms, docvaules and term dictionaries
    */
  private val PRIMARY_EXTENSIONS = Set("nvd", "dvd", "tim")

  def createMmapDirectory(path: Path): MMapDirectory = new MMapDirectory(path)

  /**
    * Creates an hybrid Lucene Directory subclass that Maps in memory all the files with primaries extensions, all other files are served through NIOFS
    * @param path the root path
    * @return the hybrid directory
    */
  def createHybridDirectory(path: Path): FileSwitchDirectory =
    new FileSwitchDirectory(PRIMARY_EXTENSIONS.asJava, new MMapDirectory(path), new NIOFSDirectory(path), true) {

      /**
        * to avoid listall() call twice
        */
      override def listAll(): Array[String] = getPrimaryDir.listAll()
    }

}
