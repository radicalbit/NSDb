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

import akka.actor.{PoisonPill, Props, ReceiveTimeout}
import com.typesafe.config.Config
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.commit_log.RollingCommitLogFileChecker.CheckFiles
import io.radicalbit.nsdb.util.ConfigKeys._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RollingCommitLogFileWriter {

  /**
    * Force rolling no matter the criteria. For test purpose
    */
  case object ForceRolling

  def props(db: String, namespace: String, metric: String): Props =
    Props(new RollingCommitLogFileWriter(db, namespace, metric))

  private[commit_log] val fileNameSeparator = "~"
  private[commit_log] val fileNamePrefix    = "nsdb"

  private[commit_log] def nextFileName(db: String,
                                       namespace: String,
                                       metric: String,
                                       fileNames: Seq[String]): String = {

    def generateNextId: Int = {
      fileNames
        .collect {
          case name
              if name.startsWith(
                s"$fileNamePrefix$fileNameSeparator$db$fileNameSeparator$namespace$fileNameSeparator$metric") =>
            name.split(fileNameSeparator).toList.last.toInt
        }
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

  val config: Config = context.system.settings.config

  private val serializerClass = config.getString(CommitLogSerializer)
  private val directory       = config.getString(CommitLogDirectory)
  private val maxSize         = config.getLong(CommitLogMaxSize)

  private val childName = s"commit-log-checker-$db-$namespace-$metric"

  private var file: File               = _
  private var fileOS: FileOutputStream = _

  override protected implicit val serializer: CommitLogSerializer =
    Class.forName(serializerClass).newInstance().asInstanceOf[CommitLogSerializer]

  lazy val passivateAfter = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.commit-log.passivate-after").toNanos,
    TimeUnit.NANOSECONDS)

  context.setReceiveTimeout(passivateAfter)

  override def preStart(): Unit = {
    log.info("Initializing the commit log serializer {}...", serializerClass)
    val checker = context.actorOf(RollingCommitLogFileChecker.props(db, namespace, metric), childName)

    new File(directory).mkdirs()

    val existingFiles = Option(Paths.get(directory).toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(name => name.contains(s"$db$fileNameSeparator$namespace$fileNameSeparator$metric"))

    val newFileName = existingFiles match {
      case fileNames if fileNames.nonEmpty =>
        val lastFile = fileNames.maxBy(s => s.split(fileNameSeparator)(4).toInt)
        lastFile
      case fileNames => nextFileName(db, namespace, metric, fileNames.toSeq)
    }

    file = new File(s"$directory/$newFileName")
    fileOS = newOutputStream(file)

    checker ! CheckFiles(file)

    log.info("Commit log serializer {} initialized successfully.", serializerClass)
  }

  override def postStop(): Unit = {
    log.debug(s"closing input stream for file $file")
    Option(fileOS).foreach(os => os.close())
  }

  override protected def createEntry(entry: CommitLogEntry): Try[Unit] = {
    log.debug("Received the entry {}.", entry)
    val operation = Try(appendToDisk(entry))

    checkAndUpdateRollingFile(file).foreach {
      case (f, fos) =>
        file = f
        fileOS.close()
        fileOS = fos
    }

    operation
  }

  protected def appendToDisk(entry: CommitLogEntry): Unit = {
    fileOS.write(serializer.serialize(entry))
    fileOS.flush()
    log.debug("Entry {} appended successfully to the commit log file {}.", entry, file.getAbsoluteFile)
  }

  protected def checkAndUpdateRollingFile(current: File): Option[(File, FileOutputStream)] =
    if (current.length() >= maxSize) {

      val f = newFile(current)

      context.child(childName).foreach {
        log.debug(s"Sending commit log check for actual file : ${f.getName}")
        _ ! CheckFiles(f)
      }

      Some(f, newOutputStream(f))
    } else
      None

  protected def newFile(current: File): File = newFile(file.getParent)

  protected def newFile(directory: String): File = {
    val nextFile = nextFileName(db, namespace, metric, fileNames = new File(directory).listFiles().map(_.getName))
    new File(s"$directory/$nextFile")
  }

  protected def newOutputStream(file: File): FileOutputStream = new FileOutputStream(file, true)

  override def receive: Receive = super.receive orElse {
    case ReceiveTimeout =>
      context.child(childName).foreach {
        log.debug(s"Sending commit log check for actual file before passivating : ${file.getName}")
        _ ! CheckFiles(file)
      }
      self ! PoisonPill
    case ForceRolling =>
      val f = newFile(file)
      file = f
      context.child(childName).foreach {
        log.debug(s"Sending commit log check for actual file : ${f.getName}")
        _ ! CheckFiles(f)
      }
      fileOS.close()
      fileOS = newOutputStream(f)
  }
}
