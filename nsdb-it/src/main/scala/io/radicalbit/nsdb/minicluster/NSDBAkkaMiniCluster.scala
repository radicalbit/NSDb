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

package io.radicalbit.nsdb.minicluster

import akka.actor.ActorSystem
import io.radicalbit.nsdb.cluster.NSDbActors
import io.radicalbit.nsdb.common.configuration.NSDbConfigProvider

import scala.concurrent.duration._
import scala.concurrent.Await

trait NSDBAkkaMiniCluster { this: NSDbConfigProvider with NSDbActors =>

  implicit var system: ActorSystem = _

  def start(): Unit = {
    system = ActorSystem("NSDb", config)
    initTopLevelActors()
  }

  def stop(): Unit = Await.result(system.terminate(), 10.seconds)

}
