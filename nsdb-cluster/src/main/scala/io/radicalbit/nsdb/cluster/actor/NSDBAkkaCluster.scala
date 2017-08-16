package io.radicalbit.nsdb.cluster.actor

import io.radicalbit.core.{BootedCore, Core}
import io.radicalbit.nsdb.cluster.endpoint.EndpointActor

trait NSDBAkkaCluster { this: Core =>

  val endpointActor = system.actorOf(EndpointActor.props, "endpoint-actor")

}

trait ProductionCluster extends NSDBAkkaCluster with BootedCore {

}
