package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.testkit.TestKit

trait WriteInterval { this: TestKit =>

  import scala.concurrent.duration._

  lazy val interval = FiniteDuration(
    system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS) + 1.second

}
