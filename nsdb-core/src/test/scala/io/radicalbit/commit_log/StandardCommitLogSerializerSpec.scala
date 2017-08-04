package io.radicalbit.commit_log

import org.scalatest.{Matchers, WordSpec}

class StandardCommitLogSerializerSpec extends WordSpec with Matchers {

  "A CommitLogEntry" when {
    "has a single dimension" should {
      "be created correctly" in {

        val ts            = 1500909299161L
        val metric        = "test-metric"
        val dimensions    = List(("dimension1", "type1", "value1".getBytes))
        val originalEntry = InsertNewEntry(ts = ts, metric = metric, dimensions = dimensions)

        val entryService = new StandardCommitLogSerializer
        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry.dimensions.map(convert) should be(originalEntry.dimensions.map(convert))
      }
    }

    "has few dimensions" should {
      "be created correctly" in {

        val ts     = 1500909299165L
        val metric = "test1-metric"
        val dimensions = List(("dimension1", "type1", "value1".getBytes),
                              ("dimension2", "type2", "value2".getBytes),
                              ("dimension3", "type3", "value3".getBytes))
        val originalEntry = InsertNewEntry(ts = ts, metric = metric, dimensions = dimensions)

        val entryService = new StandardCommitLogSerializer
        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry.dimensions.map(convert) should be(originalEntry.dimensions.map(convert))
      }
    }
  }

  private def convert(x: (String, String, Array[Byte])): (String, String, String) = x match {
    case (m, t, v) => (m, t, new String(v))
  }
}
