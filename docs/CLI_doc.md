# CLI Documentation
NSDb exposes a Command Line Interface to perform simple SQL operations.
NSDb CLI allows to execute SQL statements such as:
- data retrieval (SELECT statements)
- data deletion (DELETE statements)
- data insertion (INSERT statements)

Furthermore users can execute schema related operations, commonly defined as **commands** :
- retrieve namespaces, metrics lists
- retrieve metric description

NSDb CLI makes use of gRPC client to communicate with NSDb cluster.

## Commands Usage
CLI launch command:
```sh
./nsdb-console --host 127.0.0.1 --port 7817 --database database_name
```
accepts 3 parameters:
```
--host : NSDb host address, default value is localhost
--port : NSDb host port, default value is 7817
--database: database name on which establish connection
```

To perform previously described operations users must select the namespace on which execute statements:
```
use namespace_name
```
If users try to execute commands or statements, without selecting the operating namespace, will get the following error:
```
Namespace must be selected
```
Existing namespaces can be retrieved using the following command:
```
show namespaces
```
Example:
```
nsdb $ show namespaces


+-----------------+
|Namespace Name   |
+-----------------+
|example_namespace|
+-----------------+
```

Existing metrics for the selected namespace can be retrieved using :
```
show metrics
```
Example:
```
nsdb $ show metrics


+-------------+
|Metric Name  |
+-------------+
|people       |
+-------------+
|famous_people|
+-------------+
```


Users can access metric schema using the below-mentioned command, returning a tabular description of metric's dimensions. Each dimension is described by name and data type.
```sh
describe metric_name

```
Results example :
```
nsdb $ describe famous_people


+---------+-------+
|Field    |Type   |
|Name     |       |
+---------+-------+
|name     |VARCHAR|
+---------+-------+
|surname  |VARCHAR|
+---------+-------+
|timestamp|BIGINT |
+---------+-------+
|value    |BIGINT |
+---------+-------+
```

## Statements Usage

SQL Statements such as `SELECT`, `INSERT`, `DELETE` from metrics can be executed using NSDb CLI. Statements' execution results are display in console using tabular representation.

Statements' syntax is the same described in NSDb SQL Dialect Documentation.

### SELECT statement

Examples:
```
nsdb $ select * from famous_people


+-------------+-----+--------+-------+
|timestamp    |value|name    |surname|
+-------------+-----+--------+-------+
|1525439380976|1    |John    |Wayne  |
+-------------+-----+--------+-------+
|1525439777854|2    |Harrison|Ford   |
+-------------+-----+--------+-------+
```

```
nsdb $ select count(*) from famous_people


+---------+-----+------------+
|timestamp|value|count(value)|
+---------+-----+------------+
|0        |2    |2           |
+---------+-----+------------+
```

```
nsdb $ select * from people


Statement executed successfully, no records to display
```

### INSERT Statement

Examples:
```
nsdb $ INSERT INTO famous_people DIM(name=John, surname='Wayne') VAL=1


+-------------+-----+----+-------+
|timestamp    |value|name|surname|
+-------------+-----+----+-------+
|1525439380976|1    |John|Wayne  |
+-------------+-----+----+-------+
```

```
nsdb $ INSERT INTO famous_people DIM(name=Harrison, surname=Ford) VAL=2


+-------------+-----+--------+-------+
|timestamp    |value|name    |surname|
+-------------+-----+--------+-------+
|1525439777854|2    |Harrison|Ford   |
+-------------+-----+--------+-------+
```

### DELETE Statement

Example:
```
nsdb $ delete from famous_people where name = John


Statement executed successfully, no records to display
```
