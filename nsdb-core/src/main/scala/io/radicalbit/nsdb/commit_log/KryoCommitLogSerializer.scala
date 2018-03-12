package io.radicalbit.nsdb.commit_log

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{CommitLogEntry, DeleteEntry, InsertEntry, RejectEntry}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

class CommitLogEntrySerializer extends Serializer[CommitLogEntry] {

  override def read(kryo: Kryo, input: Input, clazz: Class[CommitLogEntry]): CommitLogEntry = {
    val className = input.readString()
    val metric    = input.readString()

    val ts      = input.readLong()
    val dimSize = input.readInt()
    val dimensions = (1 to dimSize).map { _ =>
      val name = input.readString()
      val typ  = input.readString()
      val i    = Class.forName(typ).newInstance().asInstanceOf[IndexType[_]]
      name -> i.deserialize(input.readBytes(input.readInt())).asInstanceOf[JSerializable]
    }.toMap

    val valueClass = Class.forName(input.readString()).newInstance().asInstanceOf[IndexType[_]]
    val valueSize  = input.readInt()
    val value      = valueClass.deserialize(input.readBytes(valueSize)).asInstanceOf[JSerializable]
    val bit        = Bit(ts, value, dimensions)

    className match {
      case c if classOf[InsertEntry].getSimpleName == c => ???
      case c if classOf[DeleteEntry].getSimpleName == c => ???
      case c if classOf[RejectEntry].getSimpleName == c => ???
    }
  }

  override def write(kryo: Kryo, output: Output, entry: CommitLogEntry): Unit = {
//    output.writeString(entry.getClass.getSimpleName)
//    output.writeString(entry.metric)
//
//    //bit
//
//    val bit = entry.bit
//
//    output.writeLong(bit.timestamp)
//
//    output.writeInt(bit.dimensions.size)
//
//    bit.dimensions.foreach {
//      case (name, value) =>
//        val typ      = IndexType.fromClass(value.getClass).get
//        val rawValue = typ.serialize(value)
//
//        output.writeString(name)
//        output.writeString(typ.getClass.getCanonicalName)
//        output.writeInt(rawValue.length)
//        output.write(rawValue)
//    }
//
//    val i = IndexType.fromClass(bit.value.getClass).get
//    output.writeString(i.getClass.getCanonicalName)
//    val valueBytes = bit.value.toString.getBytes
//    output.writeInt(valueBytes.length)
//    output.write(valueBytes)ù
    ???
  }

}

/**
  * Utility class to Serialize and Deserialize a CommitLogEntry.
  * Please note that this class is not intended to be thread safe.
  */
class KryoCommitLogSerializer extends CommitLogSerializer with TypeSupport {

  val kryo = new Kryo()

  override def deserialize(entry: Array[Byte]): CommitLogEntry = {
    val input = new Input(entry)
    kryo.readObject(input, classOf[CommitLogEntry], new CommitLogEntrySerializer)
  }

  override def serialize(entry: CommitLogEntry): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out  = new Output(baos)
    kryo.writeObject(out, entry, new CommitLogEntrySerializer)
    out.close()
    baos.toByteArray
  }
}
