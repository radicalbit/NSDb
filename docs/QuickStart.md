# Natural Series DB - NSDB #

## Getting Started
### Requirements
NSDb runs on Linux and Mac OS X. To be able to run NSDb, the only requirements are:

- Java 8.x (or higher) installation.
- sbt 0.13.x.

### Build and Launch
We recommend starting NSDb using Docker by the way it is possible to package the project using sbt with command `dist`:
```bash
$ sbt dist
```

Once project packaging is completed, unzip archive created in path : `nsdb-cluster/target/universal`.
```bash
$ cd nsdb/nsdb-cluster/target/universal
$ unzip nsdb-cluster-0.1.3-SNAPSHOT.zip
$ cd nsdb-cluster-0.1.3-SNAPSHOT/bin
$ ./cluster
```
In order to check if the application is up and running properly user can call health-check API:
```bash
$ curl -f "http://localhost:9000/status"
RUNNING
```
Command Line Interface(CLI) can be launched executing in the same path:
```
$ ./nsdb-console --host 127.0.0.1 --port 7817 --database database_name
```
For a comprehensive documentation regarding NSDb CLI refer to  [CLI doc](CLI_doc.md).

### Working with Docker
Alternatively to sbt build, NSDb integrates Docker image publishing and container instantiation.
To build docker image locally execute:

```bash
$ sbt 'project nsdb-cluster' clean docker:publishLocal
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

It's also possible to run an NSDb container mounting the configuration, data, certificates and external library directories:

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
To start NSDb container:

```
$ docker-compose-up
```

## Next steps
NSDb exposes data retrieval and insertion using both:

- Web APIs see [doc](RestApis.md)
- Command Line Interface (CLI) see [doc](CLI_doc.md)

Query subscription can be achieved making use of WebSocket APIs fully described in [doc](Websocket.md).
