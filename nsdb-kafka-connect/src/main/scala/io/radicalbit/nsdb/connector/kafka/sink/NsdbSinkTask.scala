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

import java.util.{Collection => JCollection, Map => JMap}

import com.datamountaineer.kcql.Kcql
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.connector.kafka.sink.conf.NsdbConfigs
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Nsdb Sink task.
  */
class NsdbSinkTask extends SinkTask {
  private val log = LoggerFactory.getLogger(classOf[NsdbSinkTask])

  private var writer: Option[NsdbSinkWriter] = None

  /**
    * Opens a new connection against Nsdb target and setup the writer.
    **/
  override def start(props: JMap[String, String]): Unit = {
    log.info("Starting a {} task.", classOf[NsdbSinkTask].getSimpleName)
    log.info("Properties are {}.", props)

    import scala.concurrent.duration._

    writer = Some(
      new NsdbSinkWriter(
        Await.result(NSDB.connect(props.get(NsdbConfigs.NSDB_HOST), props.get(NsdbConfigs.NSDB_PORT).toInt)(
                       ExecutionContext.global),
                     10.seconds),
        kcqls = props.get(NsdbConfigs.NSDB_KCQL).split(";").map(Kcql.parse).groupBy(_.getSource),
        globalDb = Option(props.get(NsdbConfigs.NSDB_DB)),
        globalNamespace = Option(props.get(NsdbConfigs.NSDB_NAMESPACE)),
        defaultValue = Option(props.get(NsdbConfigs.NSDB_DEFAULT_VALUE))
      ))
  }

  /**
    * Forwards the SinkRecords to the writer for writing.
    **/
  override def put(records: JCollection[SinkRecord]): Unit = {
    writer.foreach(w => w.write(records.asScala.toList))
  }

  override def stop(): Unit = writer = None

  override def version(): String = AppInfoParser.getVersion

}
