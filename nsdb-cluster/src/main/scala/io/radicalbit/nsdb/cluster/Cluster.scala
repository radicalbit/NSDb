package io.radicalbit.nsdb.cluster

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.web.Web

/**
  * Run a concrete Nsdb cluster node according to the configuration provided in `confDir` folder or into the classpath
  */
object Cluster extends App with ProductionCluster with Web with NsdbSecurity with NsdbConfig with LazyLogging
