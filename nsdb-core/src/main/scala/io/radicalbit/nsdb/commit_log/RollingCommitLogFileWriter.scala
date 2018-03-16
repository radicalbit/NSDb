package io.radicalbit.nsdb.commit_log

import java.io.{File, FileOutputStream}
import java.nio.file.Paths

import akka.actor.{ActorLogging, Props}
import com.typesafe.config.Config
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry
import io.radicalbit.nsdb.util.Config._

import scala.util.Try

object RollingCommitLogFileWriter {

  def props(db: String, namespace: String) = Props(new RollingCommitLogFileWriter(db, namespace))

  private[commit_log] val FileNameSeparator = "_"

  private[commit_log] def nextFileName(db: String,
                                       namespace: String,
                                       fileNamePrefix: String,
                                       fileNameSeparator: String,
                                       fileNames: Seq[String]): String = {

    def generateNextId: Int =
      fileNames
        .collect { case name if name.startsWith(fileNamePrefix) => name.split(fileNameSeparator).last.toInt }
        .sorted
        .reverse
        .headOption
        .map(_ + 1)
        .getOrElse(0)

    f"$fileNamePrefix$fileNameSeparator$db$fileNameSeparator$namespace$fileNameSeparator$generateNextId"
  }
}

/**
  * Concrete actor extending [[CommitLogWriterActor]] whose purpose is to log [[CommitLogEntry]] on file.
  * This class is intended to be thread safe because CommitLogWriter extends the Actor trait.
  * Do no call its methods from the outside, use the protocol specified inside CommitLogWriter instead.
  *
  */
class RollingCommitLogFileWriter(db: String, namespace: String) extends CommitLogWriterActor with ActorLogging {

  import RollingCommitLogFileWriter._

  implicit val config: Config = context.system.settings.config

  private val separator       = System.getProperty("line.separator").toCharArray.head
  private val serializerClass = getString(CommitLogSerializerConf)
  private val directory       = getString(CommitLogDirectoryConf)
  private val maxSize         = getInt(CommitLogMaxSizeConf)
  private val FileNamePrefix  = "nsdb"

  log.info("Initializing the commit log serializer {}...", serializerClass)
  override protected val serializer: CommitLogSerializer =
    Class.forName(serializerClass).newInstance().asInstanceOf[CommitLogSerializer]
  log.info("Commit log serializer {} initialized successfully.", serializerClass)

  private var file: File               = _
  private var fileOS: FileOutputStream = _

  override def preStart() = {
    val existingFiles = Option(Paths.get(directory).toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(name => name.contains(s"$db$FileNameSeparator$namespace"))

    val newFileName = existingFiles match {
      case fileNames if fileNames.nonEmpty => fileNames.maxBy(s => s.split(FileNameSeparator)(3).toInt)
      case fileNames                       => nextFileName(db, namespace, FileNamePrefix, FileNameSeparator, fileNames.toSeq)
    }

    file = new File(s"$directory/$newFileName")
    fileOS = newOutputStream(file)
  }

  override protected def createEntry(entry: CommitLogEntry): Try[Unit] = {
    log.debug("Received the entry {}.", entry)

    val operation = Try(appendToDisk(entry))
    // this check can be done in an async fashion
    checkAndUpdateRollingFile(file).foreach {
      case (f, fos) =>
        file = f
        fileOS = fos
    }

    operation
  }

  protected def close(): Unit = fileOS.close()

  protected def appendToDisk(entry: CommitLogEntry): Unit = {
    fileOS.write(serializer.serialize(entry))
    fileOS.write(separator)
    fileOS.flush()
    log.debug("Entry {} appended successfully to the commit log file {}.", entry, file.getAbsoluteFile)
  }

  protected def checkAndUpdateRollingFile(current: File): Option[(File, FileOutputStream)] =
    if (current.length() >= maxSize) {
      val f = newFile(current)
      Some(f, newOutputStream(f))
    } else
      None

  protected def newFile(current: File): File = newFile(file.getParent)

  protected def newFile(directory: String): File = {
    val nextFile = nextFileName(db,
                                namespace,
                                fileNamePrefix = FileNamePrefix,
                                fileNameSeparator = FileNameSeparator,
                                fileNames = new File(directory).listFiles().map(_.getName))
    new File(s"$directory/$nextFile")
  }

  protected def newOutputStream(file: File): FileOutputStream = new FileOutputStream(file, true)
}
