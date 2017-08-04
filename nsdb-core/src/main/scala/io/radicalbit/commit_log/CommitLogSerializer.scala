package io.radicalbit.commit_log

trait CommitLogSerializer {

  def deserialize(entry: Array[Byte]): InsertNewEntry

  def serialize(entry: InsertNewEntry): Array[Byte]

}
