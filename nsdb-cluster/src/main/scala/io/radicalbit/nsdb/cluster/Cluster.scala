package io.radicalbit.nsdb.cluster

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.web.Web

object Cluster extends App with ProductionCluster with Web with NsdbSecurity with NsdbConfig with LazyLogging
