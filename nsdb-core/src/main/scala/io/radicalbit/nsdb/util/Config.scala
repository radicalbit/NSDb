package io.radicalbit.nsdb.util

import com.typesafe.config.{Config => TypeSafeConfig}

object Config {

  val CommitLogSerializerConf = "radicaldb.commit-log.serializer"
  val CommitLogWriterConf     = "radicaldb.commit-log.writer"
  val CommitLogEnabledConf    = "radicaldb.commit-log.enabled"
  val CommitLogDirectoryConf  = "radicaldb.commit-log.directory"
  val CommitLogMaxSizeConf    = "radicaldb.commit-log.max-size"

  def getString(property: String)(implicit config: TypeSafeConfig): String = config.getString(property)

  def getInt(property: String)(implicit config: TypeSafeConfig): Int = config.getInt(property)
}
