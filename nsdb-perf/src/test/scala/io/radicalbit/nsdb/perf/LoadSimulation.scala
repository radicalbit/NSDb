package io.radicalbit.nsdb.perf

import io.gatling.core.scenario.Simulation
import io.gatling.core.Predef._
import io.gatling.http.Predef._

class LoadSimulation extends Simulation with SimulationConfig {

  private[this] def fromInputStream(path: String) = {
    scala.io.Source.fromInputStream(this.getClass.getResourceAsStream(path))("UTF-8").mkString
  }

  val badgeEnvelope = fromInputStream("/badgeQuery.json")

  val httpConf = http
    .baseURL(baseURL)
    .acceptHeader("text/plain")
    .acceptEncodingHeader("gzip, deflate")

  val readScenario = scenario("Run queries for badge widgets")
    .exec(
      http(s"POST $readEndpoint")
        .post(readEndpoint)
        .body(StringBody(badgeEnvelope))
        .asJSON
        .check(status is 200))

  setUp(
    readScenario.inject(atOnceUsers(translationThreadCount))
  ).protocols(httpConf)
    .assertions(global.successfulRequests.percent.is(successPercentage))
}
