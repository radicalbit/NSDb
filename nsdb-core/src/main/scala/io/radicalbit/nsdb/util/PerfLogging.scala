package io.radicalbit.nsdb.util

import com.typesafe.scalalogging.LazyLogging

object PerfLogging extends LazyLogging {

  def logPerf[T](action: => T, label: String): T = {
    val st = System.currentTimeMillis
    val res: T = action()
    logger.info("Execution time of {}: {} ms.", label, (System.currentTimeMillis - st))
  res
  }
}
