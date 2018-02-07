package io.radicalbit.nsdb.commit_log

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import io.radicalbit.nsdb.commit_log.CommitLogEntry.Dimension
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

class BitSerializer extends Serializer[Bit] {

  private def extractDimensions(dimensions: Map[String, JSerializable]): List[Dimension] =
    dimensions.map {
      case (k, v) =>
        val i = IndexType.fromClass(v.getClass).get
        (k, i.getClass.getCanonicalName, i.serialize(v))
    }.toList

  override def write(kryo: Kryo, output: Output, bit: Bit): Unit = {
    output.writeLong(bit.timestamp)
    val dimensions = extractDimensions(bit.dimensions)

    output.writeInt(dimensions.size)
    dimensions.foreach {
      case (name, typ, value) =>
        output.writeString(name)
        output.writeString(typ)
        output.writeInt(value.length)
        output.write(value)
    }

    val i = IndexType.fromClass(bit.value.getClass).get
    output.writeString(i.getClass.getCanonicalName)
    val valueBytes = bit.value.toString.getBytes
    output.write(valueBytes.length)
    output.write(valueBytes)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[Bit]): Bit = {
    val ts      = input.readLong()
    val dimSize = input.readInt()
    val dimensions = (1 to dimSize).map { _ =>
      val name = input.readString()
      val typ  = input.readString()
      val i    = IndexType.fromClass(Class.forName(typ)).get
      name -> i.deserialize(input.readBytes(input.readInt())).asInstanceOf[JSerializable]
    }.toMap

    val valueClass = IndexType.fromClass(Class.forName(input.readString())).get
    val valueSize  = input.readInt()
    val value      = valueClass.deserialize(input.readBytes(valueSize)).asInstanceOf[JSerializable]
    Bit(ts, value, dimensions)
  }
}

/**
  * Utility class to Serialize and Deserialize a CommitLogEntry.
  * Please note that this class is not intended to be thread safe.
  */
class KryoCommitLogSerializer extends CommitLogSerializer with TypeSupport {

  val kryo = new Kryo()
  kryo.register(classOf[Bit], new BitSerializer)

  override def deserialize(entry: Array[Byte]): InsertEntry = {
    val input = new Input(entry)
    kryo.readObject(input, classOf[InsertEntry])
  }

  override def serialize(entry: CommitLogEntry): Array[Byte] = {
    val out = new Output()
    kryo.writeObject(out, entry)
    out.toBytes
  }
}
