package io.radicalbit.nsdb.connector.flink.sink

import io.radicalbit.nsdb.api.scala.{Bit, NSDB}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import scala.concurrent.ExecutionContext

import org.slf4j.LoggerFactory

class NSDBSink[IN](host: String, port: Int)(implicit converter: IN => Bit) extends RichSinkFunction[IN] {

  private val log = LoggerFactory.getLogger(classOf[NSDBSink[IN]])

  lazy implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  @transient
  private var nsdb: Option[NSDB] = None

  log.info("An NSDBSink has been created")

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    nsdb = Some(NSDB.connect(host, port))
    nsdb.foreach(x => log.info("NSDBSink {} connected", x))
  }

  @throws[Exception]
  override def invoke(value: IN): Unit = {
    nsdb.foreach { conn =>
      val convValue = converter(value)
      conn.write(convValue)
      nsdb.foreach(x => log.info("NSDBSink {} write the bit {}", x, convValue: Any))
    }
  }

  @throws[Exception]
  override def close(): Unit = {
    nsdb.foreach(_.close)
    nsdb.foreach(x => log.info("NSDBSink {} closed", x))
    nsdb = None
  }
}
