package io.radicalbit.nsdb.cluster

import org.slf4j.LoggerFactory

trait NsdbPerfLogger {

  protected val perfLogger = LoggerFactory.getLogger("perf")
}
