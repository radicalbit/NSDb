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

package io.radicalbit.nsdb.util

import com.typesafe.config.{Config => TypeSafeConfig}

object Config {

  val CommitLogSerializerConf = "nsdb.commit-log.serializer"
  val CommitLogWriterConf     = "nsdb.commit-log.writer"
  val CommitLogEnabledConf    = "nsdb.commit-log.enabled"
  val CommitLogDirectoryConf  = "nsdb.commit-log.directory"
  val CommitLogMaxSizeConf    = "nsdb.commit-log.max-size"
  val CommitLogBufferSizeConf = "nsdb.commit-log.buffer-size"

  def getString(property: String)(implicit config: TypeSafeConfig): String = config.getString(property)

  def getInt(property: String)(implicit config: TypeSafeConfig): Int = config.getInt(property)

}
