# Natural Series DB - NSDB #

## Getting Started
### Requirements
NSDb runs on Linux and Mac OS X. To be able to run NSDb, the only requirements are:

- Java 8.x (or higher) installation.
- sbt 1.x.x (or higher).

### Build from source and Launch
It is possible to package the project using sbt with command `dist`:
```bash
$ sbt dist
```

Once project packaging is completed, unzip archive created in path : `package`.
```bash
$ cd package
$ unzip nsdb-1.0.0-SNAPSHOT.zip
$ cd nsdb-1.0.0-SNAPSHOT/bin
$ ./nsdb-cluster
```
In order to check if the application is up and running properly user can call health-check API:
```bash
$ curl -f "http://localhost:9000/status"
RUNNING
```
Command Line Interface(CLI) can be launched executing in the same path:
```
$ ./nsdb-cli --host 127.0.0.1 --port 7817 --database database_name
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
      image: weareradicalbit/nsdb:1.0.0-SNAPSHOT
      environment:
        AKKA_HOSTNAME: nsdb-node-1
      ports:
        - 9010:9000
        - 9000:7817
```

It's also possible to run an NSDb container mounting the configuration, data, certificates and external library directories:

```yaml
version: '3'

services:

    nsdb:
      image: weareradicalbit/nsdb:1.0.0-SNAPSHOT
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

```bash
$ docker-compose up
```
Command Line Interface(CLI) can be launched executing:
```bash
$ docker run --rm -it tools.radicalbit.io/nsdb:1.0.0-SNAPSHOT bin/nsdb-cli --host %HOST_IP% --port 7817 --database database_name
```
where `%HOST_IP%` is the IP where NSDb is running.

For a comprehensive documentation regarding NSDb CLI refer to  [CLI doc](CLI_doc.md).

### Debian native package
It is possible to create a native Debian package of the project using sbt with command `deb`:
```bash
$ sbt deb
```

Once project packaging is completed, deb package could be found in path : `package`.
```bash
$ cd package
$ dpkg --install nsdb_1.0.0-SNAPSHOT_all.deb
$ nsdb-cluster
```
Command Line Interface(CLI) can be launched executing in the same path:
```bash
$ nsdb-cli --host 127.0.0.1 --port 7817 --database database_name
```

### Centos/RHEL native package
It is possible to create a native Centos/RHEL package of the project using sbt with command `rpm`:
```bash
$ sbt rpm
```

Once project packaging is completed, rpm package could be found in path : `package`.
```bash
$ cd package
$ yum install nsdb-1.0.0-1.noarch.rpm
$ nsdb-cluster
```
Command Line Interface(CLI) can be launched executing in the same path:
```bash
$ nsdb-cli --host 127.0.0.1 --port 7817 --database database_name
```

## Next steps
NSDb exposes data retrieval and insertion using both:

- Web APIs see [doc](RestApis.md)
- Command Line Interface (CLI) see [doc](CLI_doc.md)

Query subscription can be achieved making use of WebSocket APIs fully described in [doc](Websocket.md).
