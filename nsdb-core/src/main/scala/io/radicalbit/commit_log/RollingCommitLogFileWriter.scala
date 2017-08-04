package io.radicalbit.commit_log

import java.io.{File, FileOutputStream}

import akka.actor.{ActorLogging, Props}
import io.radicalbit.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.util.Config._

object RollingCommitLogFileWriter {

  def props = Props(new RollingCommitLogFileWriter)

  private[commit_log] val FileNameSeparator = "_"

  private[commit_log] def nextFileName(fileNamePrefix: String,
                                       fileNameSeparator: String,
                                       fileNames: Seq[String]): String = {

    def generateNextId: Int =
      fileNames
        .collect { case name if (name.startsWith(fileNamePrefix)) => name.split(fileNameSeparator).last.toInt }
        .sorted
        .reverse
        .headOption
        .map(_ + 1)
        .getOrElse(0)

    f"$fileNamePrefix$fileNameSeparator$generateNextId"
  }
}

/**
  * This class is intended to be thread safe because CommitLogWriter extends the Actor trait.
  * Do no call its methods from the outside, use the protocol specified inside CommitLogWriter instead.
  *
  */
class RollingCommitLogFileWriter extends CommitLogWriterActor with ActorLogging {

  import RollingCommitLogFileWriter._

  implicit val config = context.system.settings.config

  private val separator       = System.getProperty("line.separator").toCharArray.head
  private val serializerClass = getString(CommitLogSerializerConf)
  private val directory       = getString(CommitLogDirectoryConf)
  private val maxSize         = getInt(CommitLogMaxSizeConf)
  private val FileNamePrefix  = "radicalbitdb"

  log.info("Initializing the commit log serializer {}...", serializerClass)
  override protected val serializer = Class.forName(serializerClass).newInstance().asInstanceOf[CommitLogSerializer]
  log.info("Commit log serializer {} initialized successfully.", serializerClass)

  private var file   = newFile(directory)
  private var fileOS = newOutputStream(file)

  override protected def createEntry(entry: InsertNewEntry): Unit = {
    log.debug("Received the entry {}.", entry)
    appendToDisk(entry)
    sender() ! WroteToCommitLogAck(ts = entry.ts, metric = entry.metric)

    // this check can be done in an async fashion
    checkAndUpdateRollingFile(file).foreach {
      case (f, fos) =>
        file = f
        fileOS = fos
    }
  }

  override protected def deleteEntry(commitLogEntry: DeleteExistingEntry): Unit = {}

  protected def close: Unit = fileOS.close()

  protected def appendToDisk(entry: InsertNewEntry) = {
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

  protected def newFile(current: File): File = newFile(file.getPath)

  protected def newFile(directory: String): File =
    new File(
      nextFileName(fileNamePrefix = FileNamePrefix,
                   fileNameSeparator = FileNameSeparator,
                   fileNames = new File(directory).listFiles().map(_.getName)))

  protected def newOutputStream(file: File): FileOutputStream = new FileOutputStream(file)
}
