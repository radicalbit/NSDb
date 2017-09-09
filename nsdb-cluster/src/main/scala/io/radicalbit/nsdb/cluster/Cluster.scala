package io.radicalbit.nsdb.cluster

import io.radicalbit.nsdb.cluster.actor.ProductionCluster
import io.radicalbit.nsdb.web.Web

object Cluster extends App with ProductionCluster with Web
