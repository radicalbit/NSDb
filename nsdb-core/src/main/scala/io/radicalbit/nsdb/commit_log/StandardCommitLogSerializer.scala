package io.radicalbit.nsdb.commit_log

import java.nio.{Buffer, ByteBuffer}

import io.radicalbit.nsdb.commit_log.CommitLogEntry.Dimension
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

/**
  * Utility class to Serialize and Deserialize a CommitLogEntry.
  * Please note that this class is not intended to be thread safe.
  */

object StandardCommitLogSerializer {

  sealed trait Serializable[T] {
    def serialize(entry: T): Array[Byte]

    def deserialize(entry: Array[Byte]): T
  }

  object CommitLogEntries {

    implicit object InsertEntrySerializer extends Serializable[InsertEntry] {
      override def serialize(entry: InsertEntry): Array[Byte] = ???

      override def deserialize(entry: Array[Byte]): InsertEntry = ???
    }

    implicit object DeleteEntrySerializer extends Serializable[DeleteEntry] {
      override def serialize(entry: DeleteEntry): Array[Byte] = ???

      override def deserialize(
        entry: Array[Byte]
      ): DeleteEntry = ???
    }

    implicit object RejectEntrySerializer extends Serializable[RejectEntry] {
      override def serialize(entry: RejectEntry): Array[Byte] = ???

      override def deserialize(
        entry: Array[Byte]
      ): RejectEntry = ???
    }

    implicit object DeleteMetricSerializer extends Serializable[DeleteMetric] {
      override def serialize(entry: DeleteMetric): Array[Byte] = ???

      override def deserialize(
        entry: Array[Byte]
      ): DeleteMetric = ???
    }

    implicit object DeleteNamespaceSerializer extends Serializable[DeleteNamespace] {
      override def serialize(entry: DeleteNamespace): Array[Byte] = ???

      override def deserialize(
        entry: Array[Byte]
      ): DeleteNamespace = ???
    }

    implicit class Serialize[T](entry: T) {
      def serialize(implicit makeSerializable: Serializable[T]): Array[Byte] = {
        makeSerializable.serialize(entry)
      }
    }

    implicit class Deserialize[T](bytes: Array[Byte]) {
      def deserialize(implicit makeSerializable: Serializable[T]): T = {
        makeSerializable.deserialize(bytes)
      }
    }

  }

}

  class StandardCommitLogSerializer extends CommitLogSerializer with TypeSupport {

    import StandardCommitLogSerializer.CommitLogEntries._

    private val readByteBuffer = new ReadBuffer(5000)
    private val writeBuffer = new WriteBuffer(5000)

    private def extractDimensions(dimensions: Map[String, JSerializable]): List[Dimension] =
      dimensions.map {
        case (k, v) =>
          val i = IndexType.fromClass(v.getClass).get
          (k, i.getClass.getCanonicalName, i.serialize(v))
      }.toList

    private def createDimensions(dimensions: List[Dimension]): Map[String, JSerializable] =
      dimensions.map {
        case (n, t, v) =>
          val i = Class.forName(t).newInstance().asInstanceOf[IndexType[_]]
          n -> i.deserialize(v).asInstanceOf[JSerializable]
      }.toMap

    override def deserialize(entry: Array[Byte]): CommitLogEntry = {
      readByteBuffer.clear(entry)
      //classname
      val className = readByteBuffer.read
      // timestamp
      val ts = readByteBuffer.read.toLong
      // metric
      val metric = readByteBuffer.read
      // dimensions
      val numOfDim = readByteBuffer.getInt
      val dimensions = (for {
        _ <- 1 to numOfDim
        name = readByteBuffer.read
        typ = readByteBuffer.read
        value = new Array[Byte](readByteBuffer.getInt)
        _ = readByteBuffer.get(value)
      } yield (name, typ, value)).toList

      className match {
        case c if c == classOf[InsertEntry].getCanonicalName =>
          InsertEntry(metric = metric, timestamp = ts, Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
        case c if c == classOf[RejectEntry].getCanonicalName =>
          RejectEntry(metric = metric, timestamp = ts, Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
      }
    }

    override def serialize(entry: CommitLogEntry): Array[Byte] =
      entry.serialize

//
//    private def serialize(
//      className: String,
//      ts: Long,
//      metric: String,
//      dimensions: List[(String, String, Array[Byte])]
//    ): Array[Byte] = {
//      writeBuffer.clear()
//      //classname
//      writeBuffer.write(className)
//      // timestamp
//      writeBuffer.write(ts.toString)
//      // metric
//      writeBuffer.write(metric)
//      // dimensions
//      writeBuffer.putInt(dimensions.length)
//      dimensions.foreach {
//        case (name, typ, value) =>
//          writeBuffer.write(name)
//          writeBuffer.write(typ)
//          writeBuffer.putInt(value.length)
//          writeBuffer.put(value)
//      }
//
//      writeBuffer.array
//    }

}

abstract class BaseBuffer(maxSize: Int) {

  protected val buffer: ByteBuffer = ByteBuffer.allocate(maxSize)
}

private class WriteBuffer(maxSize: Int) extends BaseBuffer(maxSize) {

  def array: Array[Byte] = buffer.array

  def clear(): Buffer = buffer.clear()

  def put(v: Array[Byte]): ByteBuffer = buffer.put(v)

  def putInt(v: Int): ByteBuffer = buffer.putInt(v)

  def write(s: String): Unit = {
    val xs = s.getBytes
    putInt(xs.length)
    put(xs)
  }
}

private class ReadBuffer(maxSize: Int) extends BaseBuffer(maxSize) {

  def clear(array: Array[Byte]): Buffer = {
    buffer.clear()
    buffer.put(array)
    buffer.position(0)
  }

  def get(v: Array[Byte]): Unit = buffer.get(v)

  def getInt: Int = buffer.getInt

  def read: String = {
    val length = buffer.getInt
    val array  = new Array[Byte](length)
    buffer.get(array)
    new String(array)
  }
}
