package io.radicalbit.nsdb.perf

import com.typesafe.config.ConfigFactory

trait SimulationConfig {

  private val config = ConfigFactory.load().getConfig("simulation")

  val baseURL = config.getString("service.host")

  val readEndpoint = config.getString("service.read.endpoint")

  val writeEndpoint = config.getString("service.write.endpoint")

  val translationThreadCount = config.getInt("scenario.translation.thread_count")

  val successPercentage = config.getInt("scenario.percent_success")

}
