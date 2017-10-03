package io.radicalbit.nsdb

import java.util.concurrent.TimeUnit

import akka.testkit.TestKit

import scala.concurrent.duration.FiniteDuration

trait WriteInterval { this: TestKit =>

  private val interval = FiniteDuration(
    system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS).toMillis + 1000

  def waitInterval = Thread.sleep(interval)

}
