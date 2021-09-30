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

package io.radicalbit.nsdb.index
import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

class BrokenTimeSeriesIndex(failureBeforeSuccess: Int = Int.MaxValue, override val directory: BaseDirectory)
    extends TimeSeriesIndex(directory) {

  private val genericFailure = Failure(
    new RuntimeException("How could it be useful to test failures if it does not fail at all"))

  private var attempts: Int = 0

  private def generateResponse: Try[Long] =
    if (attempts <= failureBeforeSuccess) {
      attempts += 1
      genericFailure
    } else
      Success(1L)

  override def write(data: Bit)(implicit writer: IndexWriter): Try[Long] = generateResponse

  override def delete(data: Bit)(implicit writer: IndexWriter): Try[Long] = generateResponse
}
