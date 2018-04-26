package io.radicalbit.nsdb.connector.kafka.sink

import java.util.{List => JList, Map => JMap}

import io.radicalbit.nsdb.connector.kafka.sink.conf.NsdbConfigs
import org.apache.kafka.common.config.{ConfigDef, ConfigValue}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Kafka connect NSDb Sink connector
  *
  **/
class NsdbSinkConnector extends SinkConnector {

  private var configProps: JMap[String, String] = _
  private val log                               = LoggerFactory.getLogger(classOf[NsdbSinkConnector])
  private var configs: Map[String, ConfigValue] = Map.empty

  /**
    * Starts the sink given an input configuration.
    *
    * @param props Input parameter.
    **/
  override def start(props: JMap[String, String]): Unit = {
    log.info(s"Starting a ${classOf[NsdbSinkConnector].getName} source connector.")
    configProps = props
    configs = config().validate(props).asScala.map(c => (c.name(), c)).toMap
  }

  /**
    * @return the Task class to be used.
    */
  override def taskClass(): Class[NsdbSinkTask] = classOf[NsdbSinkTask]

  /**
    * @return The connector version.
    */
  override def version(): String = AppInfoParser.getVersion

  /**
    * Stops the connector.
    */
  override def stop(): Unit = {}

  /**
    * Returns a set of configurations for Tasks, producing at most count configurations.
    *
    * @param maxTasks Maximum number of configurations to generate.
    * @return Configurations for Tasks.
    */
  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = {
    val raw = configs.get(NsdbConfigs.NSDB_KCQL)
    require(raw != null && raw.isDefined, s"No ${NsdbConfigs.NSDB_KCQL} provided!")

    val kcqls  = raw.get.value().toString.split(";")
    val groups = ConnectorUtils.groupPartitions(kcqls.toList.asJava, maxTasks).asScala

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(g => g.isEmpty)
      .map(g => {
        val taskConfigs: java.util.Map[String, String] = new java.util.HashMap[String, String]
        //put the default host and port to be available in task in case they are not provided.
        taskConfigs.put(NsdbConfigs.NSDB_HOST, NsdbConfigs.NSDB_HOST_DEFAULT)
        taskConfigs.put(NsdbConfigs.NSDB_PORT, NsdbConfigs.NSDB_PORT_DEFAULT.toString)
        taskConfigs.putAll(configProps)
        taskConfigs.put(NsdbConfigs.NSDB_KCQL, g.asScala.mkString(";")) //overwrite
        taskConfigs
      })
      .asJava
  }

  /**
    * @return The [[ConfigDef]] for this connector.
    */
  override def config(): ConfigDef = NsdbConfigs.configDef
}
