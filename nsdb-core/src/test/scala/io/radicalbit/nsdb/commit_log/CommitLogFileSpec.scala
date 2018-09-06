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
import java.io.{File, FileOutputStream}
import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class CommitLogFileSpec extends WordSpec with Matchers with CommitLogSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    new File(directory).mkdirs()
  }

  "CommitLogFile" should {

    "return an empty list if all entries are finalised" in {

      val file   = new File(s"$directory/testCommitLog${UUID.randomUUID().toString}")
      val fileOS = new FileOutputStream(file, true)

      balancedEntrySeq.foreach { entry =>
        fileOS.write(serializer.serialize(entry))
        fileOS.flush()
      }

      fileOS.close()

      import CommitLogFile.CommitLogFile

      file.checkPendingEntries.size shouldBe 0

    }

    "return an nonempty list if some entries are not finalised " in {
      val file   = new File(s"$directory/testCommitLog${UUID.randomUUID().toString}")
      val fileOS = new FileOutputStream(file, true)

      unbalancedEntrySeq foreach { entry =>
        fileOS.write(serializer.serialize(entry))
      }

      fileOS.flush()
      fileOS.close()

      import CommitLogFile.CommitLogFile

      file.checkPendingEntries shouldBe Seq(id1)
    }

  }
}
