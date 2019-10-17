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

package io.radicalbit.nsdb.cluster.util

import java.io._
import java.nio.file.{Path, Paths}
import java.util.zip.{ZipEntry, ZipFile}

import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.model.Location.LocationWithCoordinates

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Contains utility methods to handle files.
  */
object FileUtils {

  private class DirectoryFilter extends FileFilter {
    override def accept(pathname: File): Boolean = pathname.isDirectory
  }

  private class MetricShardsFilter(metric: String) extends FileFilter {
    override def accept(pathname: File): Boolean = pathname.isDirectory && pathname.getName.startsWith(metric)
  }

  private val BUFFER_SIZE = 4096
  private val buffer      = new Array[Byte](BUFFER_SIZE)

  /**
    * @param path a directory.
    * @return all the first level sub directories of the given directory.
    */
  def getSubDirs(path: File): List[File] =
    Option(path.listFiles(new DirectoryFilter)).map(_.toList).getOrElse(List.empty)

  /**
    * @param path a directory.
    * @return all the first level sub directories of the given directory.
    */
  def getSubDirs(path: Path): List[File] =
    Option(path.toFile.listFiles(new DirectoryFilter)).map(_.toList).getOrElse(List.empty)

  /**
    * @param path a string path.
    * @return all the first level sub directories of the given directory.
    */
  def getSubDirs(path: String): List[File] =
    Option(new File(path).listFiles(new DirectoryFilter)).map(_.toList).getOrElse(List.empty)

  /**
    * Retrieve all the shards folder inside a base path for a metric.
    * @param basePath base path for the shards.
    * @param metric the shards metric.
    * @return all the shard folders for the given metric.
    */
  def getMetricshards(basePath: Option[Path], metric: String): Array[File] =
    basePath.map(_.toFile.listFiles(new MetricShardsFilter(metric))).getOrElse(Array.empty)

  /**
    * Unzip a file into a target folder.
    * @param source the input zip path.
    * @param targetFolder the folder to unzip the input file into.
    */
  def unzip(source: String, targetFolder: String): AnyVal = {
    if (new File(source).exists) {
      val zipFile = new ZipFile(source)
      unzipAllFile(zipFile, zipFile.entries.asScala.toList, new File(targetFolder))
    }
  }

  /**
    * retrieve a list of locations from shards store in the file system.
    * The subfolders will be scanned according to this hierarchy.
    * indexBasePath -> database -> namespace -> "shards" -> metric_[from_timestamp]_[to_timestamp].
    * @param basePath the base path to begin the scan.
    * @param nodeName the node name that will be used for creating locations.
    *                 @return a list of [[LocationWithCoordinates]].
    */
  def getLocationFromFilesystem(basePath: String, nodeName: String): List[LocationWithCoordinates] =
    FileUtils
      .getSubDirs(Paths.get(basePath))
      .flatMap { databaseDir =>
        FileUtils
          .getSubDirs(databaseDir)
          .map(f => (databaseDir.getName, f))
          .flatMap {
            case (database, namespaceDir) =>
              FileUtils
                .getSubDirs(Paths.get(namespaceDir.getAbsolutePath, "shards"))
                .collect {
                  case file if file.getName.split("_").length == 3 =>
                    val Array(metric, from, to) = file.getName.split("_")
                    (database, namespaceDir.getName, Location(metric, nodeName, from.toLong, to.toLong))
                }
          }
      }

  /**
    * unzip all the entries of a zip file into a target folder.
    *
    * @param zipFile the input zip file.
    * @param entries the entries to be extracted.
    * @param targetFolder the folder to unzip the input file into.
    */
  @tailrec
  private def unzipAllFile(zipFile: ZipFile, entries: Seq[ZipEntry], targetFolder: File): Boolean = {
    entries match {
      case entry :: tailEntries =>
        if (entry.isDirectory)
          new File(targetFolder, entry.getName).mkdirs
        else
          saveFile(zipFile.getInputStream(entry), new FileOutputStream(new File(targetFolder, entry.getName)))

        unzipAllFile(zipFile, tailEntries, targetFolder)

      case _ =>
        true
    }
  }

  /**
    * Save a file from an input stream to an output stream.
    * @param fis the input stream.
    * @param fos the output stream.
    */
  private def saveFile(fis: InputStream, fos: OutputStream) = {

    def bufferReader(fis: InputStream)(buffer: Array[Byte]) = (fis.read(buffer), buffer)

    @tailrec
    def writeToFile(reader: Array[Byte] => (Int, Array[Byte]), fos: OutputStream): Boolean = {
      val (length, data) = reader(buffer)
      if (length >= 0) {
        fos.write(data, 0, length)
        writeToFile(reader, fos)
      } else
        true
    }

    try {
      writeToFile(bufferReader(fis), fos)
    } finally {
      fis.close()
      fos.close()
    }
  }
}
