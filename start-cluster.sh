#!/usr/bin/env bash
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "$1" ]; then echo "starting local cluster";
else
export AKKA_HOSTNAME=$1
echo "starting remote cluster at address '$1'"; fi
java -cp nsdb-cluster/target/scala-2.12/nsdb-cluster.jar \
    -Dlogback.configurationFile=conf/logback.xml \
    -DconfDir="$ABSOLUTE_PATH/conf" \
    io.radicalbit.nsdb.cluster.Cluster
