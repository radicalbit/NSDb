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

package io.radicalbit.nsdb.connector.flink.sink

import io.radicalbit.nsdb.api.scala.{Bit, NSDB}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Flink sink into Nsdb. See [[RichSinkFunction]] for more details.
  * @param host Nsdb Grpc host.
  * @param port Nsdb Grpc port.
  * @param converter function that converts a flink Datastream[IN] into a [[Bit]].
  * @tparam IN Datastream type.
  */
class NSDBSink[IN](host: String, port: Int)(implicit converter: IN => Bit) extends RichSinkFunction[IN] {

  private val log = LoggerFactory.getLogger(classOf[NSDBSink[IN]])

  lazy implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  @transient
  private var nsdb: Option[NSDB] = None

  log.info("An NSDBSink has been created")

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    nsdb = Some(Await.result(NSDB.connect(host, port), 10.seconds))
    nsdb.foreach(x => log.info("NSDBSink {} connected", x))
  }

  @throws[Exception]
  override def invoke(value: IN): Unit = {
    nsdb.foreach { conn =>
      val convValue = converter(value)
      conn.write(convValue)
      log.info("NSDBSink {} write the bit {}", conn, convValue: Any)
    }
  }

  @throws[Exception]
  override def close(): Unit = {
    nsdb.foreach(conn => {
      conn.close()
      log.info("NSDBSink {} closed", conn)
    })
    nsdb = None
  }
}
