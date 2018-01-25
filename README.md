# Natural Series DB - NSDB #

### Working with Docker

To build docker image locally execute:

```bash
sbt 'project nsdb-cluster' clean docker:publishLocal
```

It's possible running a container overriding the env variable:

```yaml
version: '3'

services:

    nsdb:
      image: tools.radicalbit.io/nsdb:0.0.3-SNAPSHOT
      environment:
        HTTP_INTERFACE: 0.0.0.0
        HTTP_PORT: 9002
        AKKA_HOSTNAME: nsdb-node-1
      ports:
        - 9010:9002
        - 9000:7817
```

It's also possible to run an NSDB container mounting the configuration, data, certificates and external library directories:

```yaml
version: '3'

services:

    nsdb:
      image: tools.radicalbit.io/nsdb:0.0.3-SNAPSHOT
      volumes:
        - .conf:/opt/nsdb-cluster/conf
        - /host/data/path:/opt/nsdb-cluster/data
        - /host/ext-lib/path:/opt/nsdb-cluster/ext-lib
        - /host/certs/path:/opt/certs
      ports:
        - 9000:9000
        - 7817:7817
        - 9443:9443
```