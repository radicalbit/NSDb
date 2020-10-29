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

package io.radicalbit.nsdb.common.configuration

import com.typesafe.config.{Config, ConfigFactory}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.configuration.NSDbConfig.LowLevel._
import io.radicalbit.nsdb.test._
import org.scalatest.OneInstancePerTest

class NSDbConfigProviderSpec extends NSDbSpec with OneInstancePerTest with NSDbConfigProvider {

  override def userDefinedConfig: Config      = ConfigFactory.parseResources("nsdb-test.conf").resolve()
  override def lowLevelTemplateConfig: Config = ConfigFactory.parseResources("application-test.conf")

  "NSDbConfigProvider" should {
    "properly merge configuration files" in {

      val c1 = ConfigFactory.parseString("""
          |p1 = "a"
          |p2 = "b"
          |""".stripMargin)

      val c2 = ConfigFactory.parseString(
        """
          |p2= "b"
          |p3 = "d"
          |""".stripMargin
      )

      val mergedConf = mergeConf(c1, c2)
      mergedConf.getString("p1") shouldBe "a"
      mergedConf.getString("p2") shouldBe "b"
      mergedConf.getString("p3") shouldBe "d"
    }

    "properly populate a low level conf" in {
      config.getValue(AkkaArteryHostName) shouldBe userDefinedConfig.getValue(NSDbNodeHostName)
      config.getValue(AkkaArteryPort) shouldBe userDefinedConfig.getValue(NSDbNodePort)
      config.getValue(AkkaDDPersistenceDir) shouldBe userDefinedConfig.getValue(NSDBMetadataPath)
      config.getValue(AkkaDiscoveryNSDbEndpoints) shouldBe userDefinedConfig.getValue(NSDbClusterEndpoints)

      config.hasPath(AkkaManagementContactPointNr) shouldBe false
    }
  }

}
