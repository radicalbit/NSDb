package io.radicalbit.nsdb.cluster.extension

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
  * Akka extension that let users to retrieve the actor system FQDN.
  * @param system the [[ExtendedActorSystem]].
  */
class RemoteAddress(system: ExtendedActorSystem) extends Extension {

  def address = system.provider.getDefaultAddress
}

object RemoteAddress extends ExtensionId[RemoteAddress] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem) = new RemoteAddress(system)

  override def lookup() = RemoteAddress
}