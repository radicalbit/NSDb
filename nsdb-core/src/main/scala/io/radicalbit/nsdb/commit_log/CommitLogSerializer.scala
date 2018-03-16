package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry

/**
  * Trait to be extended by [[CommitLogEntry]] serializer
  * Implemented by [[StandardCommitLogSerializer]]
  */
trait CommitLogSerializer {

  /**
    * Deserializes an Array[Byte] into a [[CommitLogEntry]] ADT
    *
    * @param entry byte representation
    * @return a [[CommitLogEntry]]
    */
  def deserialize(entry: Array[Byte]): CommitLogEntry

  /**
    * Serializes a [[CommitLogEntry]] ADT
    *
    * @param entry a [[CommitLogEntry]]
    * @return byte representation
    */
  def serialize(entry: CommitLogEntry): Array[Byte]

}
