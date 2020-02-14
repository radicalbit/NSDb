package io.radicalbit.nsdb.split_brain

import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian

object DatabaseActorsGuardianForTest {
  case object WhoAreYou
}

class DatabaseActorsGuardianForTest extends DatabaseActorsGuardian {

  import DatabaseActorsGuardianForTest._

  override def receive: Receive = {
    case WhoAreYou =>
      log.info("Received msg WhoAreYou")
      sender() ! self.path
  }
}
