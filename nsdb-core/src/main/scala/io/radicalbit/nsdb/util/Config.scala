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
