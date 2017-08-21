package io.radicalbit.nsdb.cluster

import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.ProductionCluster
import io.radicalbit.nsdb.web.Web
import scala.concurrent.duration._

object Cluster extends App with ProductionCluster with Web {}
