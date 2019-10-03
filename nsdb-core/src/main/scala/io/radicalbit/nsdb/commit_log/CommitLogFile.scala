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
import java.io.{File, FileInputStream}

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._

import scala.collection.mutable.ListBuffer

object CommitLogFile {

  /**
    * Useful to add the check entries feature to a File
    */
  implicit class CommitLogFile(val file: File) {

    /**
      * Check if the entries of the file (It is supposed to be a valid commit log file) are balanced,
      * i.e. every [[ReceivedEntry]] or [[AccumulatedEntry]] must have its related [[PersistedEntry]] or [[RejectedEntry]]
      * @param serializer Serializer to use for deserialization purposes
      * @return the list of unbalanced entries, if exist. Empty list if the file is correctly balanced.
      */
    def checkPendingEntries(implicit serializer: CommitLogSerializer): (List[Int], List[Int]) = {
      val pending: ListBuffer[Int]       = ListBuffer.empty[Int]
      val closedEntries: ListBuffer[Int] = ListBuffer.empty[Int]

      val contents    = new Array[Byte](5000)
      val inputStream = new FileInputStream(file)
      var r           = inputStream.read(contents)
      while (r != -1) {

        val rawEntry = serializer.deserialize(contents)

        rawEntry match {
          case Some(e: ReceivedEntry)    => if (!pending.contains(e.id)) pending += e.id
          case Some(e: AccumulatedEntry) => if (!pending.contains(e.id)) pending += e.id
          case Some(e: PersistedEntry)   => if (!closedEntries.contains(e.id)) closedEntries += e.id
          case Some(e: RejectedEntry)    => if (!closedEntries.contains(e.id)) closedEntries += e.id
          case Some(_)                   =>
          case None                      =>
        }
        r = inputStream.read(contents)
      }

      inputStream.close()
      (pending.toList, closedEntries.toList)
    }

  }
}
