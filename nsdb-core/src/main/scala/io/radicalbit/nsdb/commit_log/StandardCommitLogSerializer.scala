package io.radicalbit.nsdb.commit_log

import java.nio.{Buffer, ByteBuffer}

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.{Dimension, Value}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}
import io.radicalbit.nsdb.statement.StatementParser

/**
  * Utility class to Serialize and Deserialize a CommitLogEntry.
  * Please note that this class is not intended to be thread safe.
  */
class StandardCommitLogSerializer extends CommitLogSerializer with TypeSupport {

  private val readByteBuffer = new ReadBuffer(5000)
  private val writeBuffer    = new WriteBuffer(5000)

  private def extractDimensions(dimensions: Map[String, JSerializable]): List[Dimension] =
    dimensions.map {
      case (k, v) =>
        val i = IndexType.fromClass(v.getClass).get
        (k, i.getClass.getCanonicalName, i.serialize(v))
    }.toList

  private def extractValue(value: JSerializable): Value = {
    val vType = IndexType.fromClass(value.getClass).get
    ("value", vType.getClass.getCanonicalName, vType.serialize(value))
  }

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
    // database
    val db = readByteBuffer.read
    //  namespace
    val namespace = readByteBuffer.read
    // metric
    val metric = readByteBuffer.read
    // dimensions
    val numOfDim = readByteBuffer.getInt
    val dimensions = (for {
      _ <- 1 to numOfDim
      name  = readByteBuffer.read
      typ   = readByteBuffer.read
      value = new Array[Byte](readByteBuffer.getInt)
      _     = readByteBuffer.get(value)
    } yield (name, typ, value)).toList

    className match {
      case c if c == classOf[InsertEntry].getCanonicalName =>
        InsertEntry(db = db,
          namespace = namespace,
          metric = metric,
                    timestamp = ts,
                    Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
      case c if c == classOf[RejectEntry].getCanonicalName =>
        ???
//        RejectEntry(metric = metric,
//                    timestamp = ts,
//                    Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
    }
  }

  override def serialize(entry: CommitLogEntry): Array[Byte] =
    entry match {
      case e: InsertEntry => serializeEntry(e.getClass.getCanonicalName, e.timestamp, e.db, e .namespace, e.metric, extractValue(e.bit.value), extractDimensions(e.bit.dimensions))
      case e: RejectEntry => serializeEntry(e.getClass.getCanonicalName, e.timestamp, e.db, e .namespace, e.metric, extractValue(e.bit.value), extractDimensions(e.bit.dimensions))
      case e: DeleteEntry => ???
      case e: DeleteNamespaceEntry =>
      case e: DeleteMetricEntry =>
    }


    private def serializeEntry(
      className: String,
      ts: Long,
      db: String,
      namespace: String,
      metric: String,
      value: Value,
      dimensions: List[Dimension]
    ): Array[Byte] = {
      writeBuffer.clear()
      //classname
      writeBuffer.write(className)
      // timestamp
      writeBuffer.write(ts.toString)
      //db
      writeBuffer.write(db)
      //namespace
      writeBuffer.write(namespace)
      // metric
      writeBuffer.write(metric)
      // dimensions
      writeBuffer.putInt(dimensions.length)
      dimensions.foreach {
        case (name, typ, value) =>
          writeBuffer.write(name)
          writeBuffer.write(typ)
          writeBuffer.putInt(value.length)
          writeBuffer.put(value)
      }

      writeBuffer.array
    }

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
