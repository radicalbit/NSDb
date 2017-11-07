#!/usr/bin/env bash
if [ -z "$1" ]; then echo "starting local cluster";
else
export AKKA_HOSTNAME=$1
echo "starting remote cluster ad address '$1'"; fi
java -cp nsdb-cluster/target/scala-2.12/nsdb-cluster.jar \
    -Dlogback.configurationFile=conf/logback.xml \
    io.radicalbit.nsdb.cluster.Cluster
