/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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
