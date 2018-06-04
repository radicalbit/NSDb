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

package io.radicalbit.nsdb.connector.kafka.sink.conf

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Sink configuration parameters
  */
object NsdbConfigs {

  val NSDB_HOST         = "nsdb.host"
  val NSDB_HOST_DOC     = "Nsdb host"
  val NSDB_HOST_DEFAULT = "localhost"

  val NSDB_PORT         = "nsdb.port"
  val NSDB_PORT_DOC     = "Nsdb port"
  val NSDB_PORT_DEFAULT = 7817

  val NSDB_KCQL     = "nsdb.kcql"
  val NSDB_KCQL_DOC = "Kcql semicolon separated list"

  val NSDB_DB     = "nsdb.db"
  val NSDB_DB_DOC = "Nsdb db (optional)"

  val NSDB_NAMESPACE     = "nsdb.namespace"
  val NSDB_NAMESPACE_DOC = "Nsdb namespace (optional)"

  val NSDB_DEFAULT_VALUE     = "nsdb.defaultValue"
  val NSDB_DEFAULT_VALUE_DOC = "Nsdb default value (optional)"

  /**
    * @return sink expected configuration:
    *
    *         - nsdb.host nsdb hosts.
    *
    *         - nsdb.port nsdb port.
    *
    *         - nsdb.kcql semicolon separated list of kcql statements to filter data from topics.
    *
    *         - nsdb.db the nsdb db to store records in case a mapping in the kcql is not defined
    *
    *         -nsdb.namespace the nsdb db to store records in case a mapping in the kcql is not defined
    *
    *         -nsdb.defaultValue the value to be used in case a mapping in the kcql is not defined
    */
  def configDef: ConfigDef =
    new ConfigDef()
      .define(NSDB_HOST,
              Type.STRING,
              NSDB_HOST_DEFAULT,
              Importance.HIGH,
              NSDB_HOST_DOC,
              "Connection",
              1,
              ConfigDef.Width.MEDIUM,
              NSDB_HOST)
      .define(NSDB_PORT,
              Type.INT,
              NSDB_PORT_DEFAULT,
              Importance.MEDIUM,
              NSDB_PORT_DOC,
              "Connection",
              2,
              ConfigDef.Width.MEDIUM,
              NSDB_PORT)
      .define(NSDB_KCQL,
              Type.STRING,
              Importance.HIGH,
              NSDB_KCQL_DOC,
              "Connection",
              3,
              ConfigDef.Width.MEDIUM,
              NSDB_KCQL)
      .define(NSDB_DB, Type.STRING, null, Importance.MEDIUM, NSDB_DB_DOC)
      .define(NSDB_NAMESPACE, Type.STRING, null, Importance.MEDIUM, NSDB_NAMESPACE_DOC)
      .define(NSDB_DEFAULT_VALUE, Type.STRING, null, Importance.MEDIUM, NSDB_DEFAULT_VALUE_DOC)

}
