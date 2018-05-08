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

package io.radicalbit.nsdb.connector.kafka.sink

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging
import io.radicalbit.nsdb.api.scala.{Bit, Db, NSDB}
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

/**
  * Handles writes to Nsdb.
  */
class NsdbSinkWriter(connection: NSDB,
                     kcqls: Map[String, Array[Kcql]],
                     globalDb: Option[String],
                     globalNamespace: Option[String])
    extends StrictLogging {
  logger.info("Initialising Nsdb writer")

  /**
    * Write a list of SinkRecords to Nsdb.
    *
    * @param records The list of SinkRecords to write.
    **/
  def write(records: List[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      grouped.foreach({
        case (topic, entries) =>
          writeRecords(topic, entries, kcqls.getOrElse(topic, Array.empty), globalDb, globalNamespace)
      })
    }
  }

  /**
    * Write a list of sink records to Nsdb.
    *
    * @param topic   The source topic.
    * @param records The list of sink records to write.
    **/
  private def writeRecords(topic: String,
                           records: List[SinkRecord],
                           kcqls: Array[Kcql],
                           globalDb: Option[String],
                           globalNamespace: Option[String]): Unit = {
    logger.debug(s"Handling records for $topic")

    import NsdbSinkWriter._

    val writes = records.map(parse)

    kcqls.foreach(kcql => {

      val parsedKcql = ParsedKcql(kcql, globalDb, globalNamespace)

      val bitSeq = writes.map(map => {
        convertToBit(parsedKcql, map)
      })

      connection.write(bitSeq)

      logger.debug(s"Wrote ${writes.length} to nsdb.")
    })

  }

  def close(): Unit = connection.close()
}

object NsdbSinkWriter {

  private def getFieldName(parent: Option[String], field: String) = parent.map(p => s"$p.$field").getOrElse(field)

  /**
    * Recursively build a Map to represent a field.
    *
    * @param field  The field schema to add.
    * @param struct The struct to extract the value from.
    **/
  private def buildField(field: Field,
                         struct: Struct,
                         parentField: Option[String] = None,
                         acc: Map[String, Any] = Map.empty): Map[String, Any] = {
    field.schema().`type`() match {
      case Type.STRUCT =>
        val nested = struct.getStruct(field.name())
        val schema = nested.schema()
        val fields = schema.fields().asScala
        fields.flatMap(f => buildField(f, nested, Some(field.name()))).toMap

      case Type.BYTES =>
        if (field.schema().name() == Decimal.LOGICAL_NAME) {
          val decimal = Decimal.toLogical(field.schema(), struct.getBytes(field.name()))
          acc ++ Map(getFieldName(parentField, field.name()) -> decimal)
        } else {
          val str = new String(struct.getBytes(field.name()), "utf-8")
          acc ++ Map(getFieldName(parentField, field.name()) -> str)
        }

      case _ =>
        val value = field.schema().name() match {
          case Time.LOGICAL_NAME      => Time.toLogical(field.schema(), struct.getInt32(field.name()))
          case Timestamp.LOGICAL_NAME => Timestamp.toLogical(field.schema(), struct.getInt64(field.name()))
          case Date.LOGICAL_NAME      => Date.toLogical(field.schema(), struct.getInt32(field.name()))
          case Decimal.LOGICAL_NAME   => Decimal.toLogical(field.schema(), struct.getBytes(field.name()))
          case _                      => struct.get(field.name())
        }
        acc ++ Map(getFieldName(parentField, field.name()) -> value)
    }
  }

  def parse(record: SinkRecord): Map[String, Any] = {
    val schema = record.valueSchema()
    if (schema == null) {
      sys.error("Schemaless records are not supported")
    } else {
      schema.`type`() match {
        case Schema.Type.STRUCT =>
          val s      = record.value().asInstanceOf[Struct]
          val fields = schema.fields().asScala
          fields.flatMap(f => buildField(f, s)).toMap
        case other => sys.error(s"$other schema is not supported")
      }
    }
  }

  /**
    * Converts values gathered from topic record into a NSdb [[Bit]]
    * @param parsedKcql Parsed kcql configurations.
    * @param valuesMap Keuy value maps retrieved from a topic record.
    * @return Nsdb Bit built on input configurations and topic data.
    */
  private[sink] def convertToBit(parsedKcql: ParsedKcql, valuesMap: Map[String, Any]): Bit = {

    val dbField        = parsedKcql.dbField
    val namespaceField = parsedKcql.namespaceField
    val aliasMap       = parsedKcql.aliasesMap

    require(
      valuesMap.get(parsedKcql.dbField).isDefined && valuesMap(parsedKcql.dbField).isInstanceOf[String],
      s"required field $dbField is missing from record or is invalid"
    )
    require(valuesMap.get(namespaceField).isDefined,
            s"required field $namespaceField is missing from record or is invalid")

    var bit: Bit = Db(valuesMap(dbField).toString).namespace(valuesMap(namespaceField).toString).bit(parsedKcql.metric)

    val timestampField = aliasMap("timestamp")
    val valueField     = aliasMap("value")

    valuesMap.get(timestampField) match {
      case Some(t: Long) => bit = bit.timestamp(t)
      case Some(v)       => sys.error(s"Type ${v.getClass} is not supported for timestamp field")
      case None          => sys.error(s"Timestamp is not defined in record")
    }

    valuesMap.get(valueField) match {
      case Some(v: Int)    => bit = bit.value(v)
      case Some(v: Long)   => bit = bit.value(v)
      case Some(v: Double) => bit = bit.value(v)
      case Some(v: Float)  => bit = bit.value(v)
      case Some(v)         => sys.error(s"Type ${v.getClass} is not supported for value field")
      case None            => sys.error(s"Value is not defined in record")
    }

    (valuesMap - timestampField - valueField - dbField - namespaceField).foreach {
      case (name, value) =>
        value match {
          case v: Int    => bit = bit.dimension(name, v)
          case v: Long   => bit = bit.dimension(name, v)
          case v: Double => bit = bit.dimension(name, v)
          case v: Float  => bit = bit.dimension(name, v)
          case v: String => bit = bit.dimension(name, v)
          case v         => sys.error(s"Type ${v.getClass} is not supported for dimensions")
        }
    }
    bit
  }

}
