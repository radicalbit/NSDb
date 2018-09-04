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

package io.radicalbit.nsdb.commit_log

import java.io._
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.Props
import com.typesafe.config.Config
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.util.Config._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RollingCommitLogFileWriter {

  /**
    * Force rolling no matter the criteria. For test purpose
    */
  case object ForceRolling

  def props(db: String, namespace: String, metric: String) =
    Props(new RollingCommitLogFileWriter(db, namespace, metric))

  private[commit_log] val fileNameSeparator = "~"
  private[commit_log] val fileNamePrefix    = "nsdb"

  private[commit_log] def nextFileName(db: String, namespace: String, metric: String, fileNames: Seq[String]): String = {

    def generateNextId: Int = {
      fileNames
        .collect { case name if name.startsWith(fileNamePrefix) => name.split(fileNameSeparator).toList.last.toInt }
        .sorted
        .reverse
        .headOption
        .map(_ + 1)
        .getOrElse(0)
    }

    f"$fileNamePrefix$fileNameSeparator$db$fileNameSeparator$namespace$fileNameSeparator$metric$fileNameSeparator$generateNextId"
  }
}

/**
  * Concrete actor extending [[CommitLogWriterActor]] whose purpose is to log [[CommitLogEntry]] on file.
  * This class is intended to be thread safe because CommitLogWriter extends the Actor trait.
  * Do no call its methods from the outside, use the protocol specified inside CommitLogWriter instead.
  */
class RollingCommitLogFileWriter(db: String, namespace: String, metric: String) extends CommitLogWriterActor {

  import RollingCommitLogFileWriter._

  implicit val config: Config = context.system.settings.config

  private val serializerClass = getString(CommitLogSerializerConf)
  private val directory       = getString(CommitLogDirectoryConf)
  private val maxSize         = getLong(CommitLogMaxSizeConf)

  log.info("Initializing the commit log serializer {}...", serializerClass)
  override protected implicit val serializer: CommitLogSerializer =
    Class.forName(serializerClass).newInstance().asInstanceOf[CommitLogSerializer]
  log.info("Commit log serializer {} initialized successfully.", serializerClass)

  private var file: File                 = _
  private var fileOS: OutputStreamWriter = _

  private val oldFilesToCheck: ListBuffer[File]                   = ListBuffer.empty
  private val pendingOutdatedEntries: mutable.Map[File, Seq[Int]] = mutable.Map.empty
  private val pendingFinalizingEntries: ListBuffer[Int]           = ListBuffer.empty

  override def preStart(): Unit = {

    new File(directory).mkdirs()

    val existingFiles = Option(Paths.get(directory).toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(name => name.contains(s"$db$fileNameSeparator$namespace$fileNameSeparator$metric"))

    val newFileName = existingFiles match {
      case fileNames if fileNames.nonEmpty =>
        val lastFile = fileNames.maxBy(s => s.split(fileNameSeparator)(4).toInt)
        oldFilesToCheck ++= (fileNames - lastFile).map(name => new File(s"$directory/$name"))
        lastFile
      case fileNames => nextFileName(db, namespace, metric, fileNames.toSeq)
    }

    file = new File(s"$directory/$newFileName")
    fileOS = newOutputStream(file)

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.commit-log.check-interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    import context.dispatcher

    /**
      * Checks if the old commit log files can be safely deleted in order to preserve disk space.
      * Basically, checks if a file is balanced (every entry is finalised), if this is positive, the file can be safely deleted,
      * otherwise all the non balanced entries must be stored in an auxiliary memory queue, that will be popped every time a finalisation entry comes.
      * When the memory queue is balanced (there is only one placeholder for a given file) the given file can easily be removed as well.
      */
    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {

      import CommitLogFile._

      oldFilesToCheck.foreach (file => {
        val pendingEntries = file.checkPendingEntries
        if (pendingEntries.isEmpty) {
          file.delete()
          oldFilesToCheck -= file
        } else {
          pendingOutdatedEntries += (file -> pendingEntries)
        }
        ()
      })

      val adjustedFiles = pendingOutdatedEntries.collect {
        case (f: File, e: Seq[Int]) if e.forall(pendingFinalizingEntries.contains(_)) => f
      }

      adjustedFiles.foreach { f =>
        f.delete()
        oldFilesToCheck -= f
        pendingOutdatedEntries -= f
      }
      pendingFinalizingEntries.clear()
    }
  }

  override protected def createEntry(entry: CommitLogEntry): Try[Unit] = {
    log.debug("Received the entry {}.", entry)

    entry match {
      case e: FinalizationEntry =>
        pendingFinalizingEntries += e.id

      case _ => //nothing to do here
    }

    val operation = Try(appendToDisk(entry))

    checkAndUpdateRollingFile(file).foreach {
      case (f, fos) =>
        file = f
        fileOS.close()
        fileOS = fos
    }

    operation
  }

  protected def close(): Unit = fileOS.close()

  protected def appendToDisk(entry: CommitLogEntry): Unit = {
    val byt = serializer.serialize(entry)
    fileOS.write(new String(serializer.serialize(entry)))
    fileOS.flush()
    log.debug("Entry {} appended successfully to the commit log file {}.", entry, file.getAbsoluteFile)
  }

  protected def checkAndUpdateRollingFile(current: File): Option[(File, OutputStreamWriter)] =
    if (current.length() >= maxSize) {

      val f = newFile(current)

      oldFilesToCheck += current

      Some(f, newOutputStream(f))
    } else
      None

  protected def newFile(current: File): File = newFile(file.getParent)

  protected def newFile(directory: String): File = {
    val nextFile = nextFileName(db, namespace, metric, fileNames = new File(directory).listFiles().map(_.getName))
    new File(s"$directory/$nextFile")
  }

  protected def newOutputStream(file: File): OutputStreamWriter =
    new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8")

  override def receive: Receive = super.receive orElse {
    case ForceRolling =>
      val f = newFile(file)
      oldFilesToCheck += file
      file = f
      fileOS.close()
      fileOS = newOutputStream(f)
  }
}
