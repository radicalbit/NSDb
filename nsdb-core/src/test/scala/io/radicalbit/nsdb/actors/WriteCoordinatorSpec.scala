package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class WriteCoordinatorSpec
    extends TestKit(ActorSystem("ignorantodb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  "A partition" when {
    "inserted" should {
      "be saved correctly" in {}
    }
  }

}
