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

package io.radicalbit.nsdb.commit_log
import java.io.File

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object CommitLogFile {

  implicit class CommitLogFile(val file: File) {

    def checkPendingEntries(implicit serializer: CommitLogSerializer): List[Int] = {
      val pending: ListBuffer[Int] = ListBuffer.empty[Int]

      Source.fromFile(file).getLines().foreach { l =>
        val rawEntry = serializer.deserialize(l.getBytes)

        rawEntry match {
          case bitEntry: CommitLogBitEntry =>
            bitEntry match {
              case e: ReceivedEntry    => if (!pending.contains(e.id)) pending += e.id
              case e: AccumulatedEntry => if (!pending.contains(e.id)) pending += e.id
              case e: PersistedEntry   => pending.remove(pending.indexOf(e.id))
              case e: RejectedEntry    => pending.remove(pending.indexOf(e.id))
            }
          case _ =>
        }

      }
      pending.toList
    }

  }
}
