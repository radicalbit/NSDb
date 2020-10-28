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

package io.radicalbit.nsdb.commit_log
import java.io.{File, FileOutputStream}
import java.util.UUID

import io.radicalbit.nsdb.test.NSDbSpec
import org.scalatest.BeforeAndAfterAll

class CommitLogFileSpec extends NSDbSpec with CommitLogSpec with BeforeAndAfterAll {

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

      file.checkPendingEntries._1.size shouldBe 3
      file.checkPendingEntries._2.size shouldBe 3

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

      file.checkPendingEntries._1 shouldBe Seq(id0, id1, id2)
      file.checkPendingEntries._2 shouldBe Seq(id0, id2)
    }

  }
}
