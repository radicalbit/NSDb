#!/usr/bin/env bash
java -cp nsdb-http/target/scala-2.12/nsdb-http-assembly-0.0.1-SNAPSHOT.jar io.radicalbit.nsdb.web.client.WSClientApp "$@"
