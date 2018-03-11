package io.radicalbit.nsdb.cluster

import org.slf4j.LoggerFactory

/**
  * Provides a performance logger instance
  */
trait NsdbPerfLogger {

  protected val perfLogger = LoggerFactory.getLogger("perf")
}
