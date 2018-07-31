package io.radicalbit.nsdb.cluster.minicluster

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cluster.ProductionCluster

trait NsdbMiniClusterDefinition extends ProductionCluster with NsdbMiniClusterConf with LazyLogging {}
