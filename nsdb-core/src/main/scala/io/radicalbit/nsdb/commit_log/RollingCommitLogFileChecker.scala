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

package io.radicalbit.nsdb.commit_log

import java.io.File
import java.nio.file.Paths

import akka.actor.Props
import com.typesafe.config.Config
import io.radicalbit.nsdb.commit_log.RollingCommitLogFileChecker.CheckFiles
import io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter.fileNameSeparator
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.protocol.NSDbSerializable

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RollingCommitLogFileChecker {
  def props(db: String, namespace: String, metric: String): Props =
    Props(new RollingCommitLogFileChecker(db, namespace, metric))

  case class CheckFiles(actualFile: File) extends NSDbSerializable
}

/**
  * This Actor handles old commit log files deletion mechanism
  *
  * @param db database name
  * @param namespace namespace name
  * @param metric metric name
  */
class RollingCommitLogFileChecker(db: String, namespace: String, metric: String) extends ActorPathLogging {

  val config: Config = context.system.settings.config

  private val directory       = config.getString(CommitLogDirectory)
  private val serializerClass = config.getString(CommitLogSerializer)

  implicit val serializer: CommitLogSerializer =
    Class.forName(serializerClass).newInstance().asInstanceOf[CommitLogSerializer]

  val pendingOutdatedEntries: mutable.Map[String, (ListBuffer[Int], ListBuffer[Int])] = mutable.Map.empty

  private def isOlder(fileName: String, actualFileName: String): Boolean = {
    fileName.split(fileNameSeparator).toList.last.toInt < actualFileName.split(fileNameSeparator).toList.last.toInt
  }

  override def receive: Receive = {
    case CheckFiles(actualFile) =>
      log.debug(s"Received commitlog check for actual file : ${actualFile.getName}")
      val existingOldFileNames: List[String] = Option(Paths.get(directory).toFile.list())
        .map(_.toSet)
        .getOrElse(Set.empty)
        .filter(name =>
          name.contains(s"$db$fileNameSeparator$namespace$fileNameSeparator$metric") && isOlder(name,
                                                                                                actualFile.getName))
        .toList
        .sortBy(_.split(fileNameSeparator).toList.last.toInt)

      log.debug(s"Old files to be checked: $existingOldFileNames")

      import CommitLogFile._

      existingOldFileNames.foreach(fileName => {
        val processedFile                   = new File(s"$directory/$fileName")
        val (pendingEntries, closedEntries) = processedFile.checkPendingEntries

        pendingOutdatedEntries.get(fileName) match {
          case None =>
            pendingOutdatedEntries += (fileName -> (pendingEntries.to[ListBuffer], closedEntries.to[ListBuffer]))
          case Some(_) =>
        }

        closedEntries.foreach {
          closedEntry =>
            pendingOutdatedEntries.foreach {
              case (_, (pending, _)) =>
                if (pending.toList.contains(closedEntry)) {
                  log.debug(s"removing entry: $closedEntry in file $fileName processing file: $fileName")
                  pendingOutdatedEntries(fileName)._1 -= closedEntry
                  pendingOutdatedEntries(fileName)._2 -= closedEntry
                  pending -= closedEntry
                }
                log.debug(s"pending entries for file: $fileName are : ${pending.size}")
            }
        }
      })
      pendingOutdatedEntries.foreach {
        case (fileName, (pending, _)) if pending.isEmpty =>
          log.debug(s"deleting file: $fileName")
          pendingOutdatedEntries -= fileName
          new File(s"$directory/$fileName").delete()
        case _ =>
      }
    case msg =>
      log.error(s"Unexpected message: $msg")
  }
}
