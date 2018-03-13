//package io.radicalbit.nsdb.commit_log
//
//import io.radicalbit.nsdb.common.JSerializable
//import io.radicalbit.nsdb.common.protocol.Bit
//import org.scalatest.{Matchers, WordSpec}
//import StandardCommitLogSerializer.CommitLogEntries._
//
//class StandardCommitLogSerializerSpec extends WordSpec with Matchers {
//
//  "A CommitLogEntry" when {
//    "has a single dimension" should {
//      "be created correctly" in {
//
//        val ts            = 1500909299161L
//        val metric        = "test-metric"
//        val bit           = Bit(ts, 0, Map("dimension1" -> "value1"))
//        val originalEntry = InsertEntry(metric = metric, timestamp = bit.timestamp, bit = bit)
//
//        val entryService = new StandardCommitLogSerializer
//        val serByteArray = originalEntry.serialize
//        val desEntry     = serByteArray.deserialize
//
//        desEntry should be(originalEntry)
//      }
//    }
//
//    "has few dimensions" should {
//      "be created correctly" in {
//
//        val ts     = 1500909299165L
//        val metric = "test1-metric"
//        val dimensions: Map[String, JSerializable] =
//          Map("dimension1" -> "value1", "dimension2" -> 2, "dimension3" -> 3L)
//        val bit           = Bit(timestamp = ts, dimensions = dimensions, value = 0)
//        val originalEntry = InsertEntry(metric = metric, timestamp = bit.timestamp, bit = bit)
//
//        val entryService = new StandardCommitLogSerializer
//        val serByteArray = entryService.serialize(originalEntry)
//        val desEntry     = entryService.deserialize(serByteArray)
//
//        desEntry should be(originalEntry)
//      }
//    }
//  }
//
//}
