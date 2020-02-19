package io.radicalbit.nsdb.split_brain

import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian
import io.radicalbit.nsdb.common.protocol.NSDbSerializable

object DatabaseActorsGuardianForTest {
  case object WhoAreYou extends NSDbSerializable
}

/**
 * Mock Database Actor Guardian for test
 */
class DatabaseActorsGuardianForTest extends DatabaseActorsGuardian {

  import DatabaseActorsGuardianForTest._

  override def receive: Receive = {
    case WhoAreYou =>
      log.info("Received msg WhoAreYou")
      sender() ! self.path
  }
}
