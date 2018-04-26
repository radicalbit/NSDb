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
    log.info(s"Starting a ${classOf[NsdbSinkTask].getSimpleName} task.")
    log.info("Properties are {}.", props)

    import scala.concurrent.duration._

    writer = Some(
      new NsdbSinkWriter(
        Await.result(NSDB.connect(props.get(NsdbConfigs.NSDB_HOST), props.get(NsdbConfigs.NSDB_PORT).toInt)(
                       ExecutionContext.global),
                     10.seconds),
        kcqls = props.get(NsdbConfigs.NSDB_KCQL).split(";").map(Kcql.parse).groupBy(_.getSource),
        globalDb = Option(props.get(NsdbConfigs.NSDB_DB)),
        globalNamespace = Option(props.get(NsdbConfigs.NSDB_NAMESPACE))
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
