NSDb Kafka Sink allows you to write events from Kafka to Nsdb.
The connector takes the value from the Kafka Connect SinkRecords and inserts a new bit into NSDb.
Prerequisites

- Apache Kafka 0.10.x or above
- Kafka Connect 0.10.x or above
- NSDb 0.1.3 or above

# Configurations

## General Connector Configuration
The Kafka Connect framework requires the following in addition to any connectors specific configurations:

Name  | Description  | Type  | Value
--|---|---|--
name  |  Name of the connector | String  |  Anything unique across the Connect cluster
topics  | The topics to sink | String | comma separated list of topics used in the connector
tasks.max  | The number of tasks to be created across the connect cluster  | Int | Default value is 1
connector.class  | Connector FQCN  |  String | io.radicalbit.nsdb.connector.kafka.sink.NsdbSinkConnector

## Specific Nsdb Sink Configuration
Name  | Description  | Type  | Value
--|---|---|--
nsdb.host  | Hostname of the NSDb instance to connect to | String | default value is `localhost`
nsdb.port  | Port of the NSDb instance to connect to | Int | default value is `7817`
nsdb.kcql  | Kcql expressions used to map topic data to NSDb bits | String  | semicolon separated Kcql expressions
nsdb.db  | NSDb db to use in case no mappig is provided in the Kcql | String  |  If a mapping is provided in the Kcql this config will be overridden
nsdb.namespace  | NSDb db to use in case no mappig is provided in the Kcql | String  | If a mapping is provided in the Kcql this config will be overridden
nsdb.defaultValue | default value | Numeric | if a value alias is provided in the Kcql expression this config will be ignored 

## KCQL Support

The NSDb sink supports KCQL, [Kafka Connect Query Language](https://github.com/Landoop/kafka-connect-query-language).

The following capabilites can be achieved using KCQL:

- Dimensions selection and mapping
- Value mapping
- Timestamp Mapping
- Target bit selection
- Target db and namespace selection (if not specified, global configurations will be used)

```sql
-- Select field x as dimension a, field y as value and z as the timestamp from topicA to bitA
INSERT INTO bitA SELECT x AS a, y AS value FROM topicA WITHTIMESTAMP z

-- Select field x as dimension a, field z as dimension c, field y as value and the current time as the timestamp from topicB to bitB
INSERT INTO bitB SELECT x AS a, y AS value, z as c FROM topicB WITHTIMESTAMP sys_time()

-- Select field d as the db, field n as the namespace, field x as dimension a, field z as dimension c, field y as value and the current time as the timestamp from topicB to bitB
INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as c FROM topicC WITHTIMESTAMP sys_time()
```

## example
```bash
echo '{"name":"manufacturing-nsdb-sink",
"config": {"connector.class":"io.radicalbit.nsdb.connector.kafka.sink.NsdbSinkConnector",
"tasks.max":"1","nsdb.host":"nsdbhost",
"topics":"topicA, topicB, topicC",
"nsdb.kcql":
"INSERT INTO bitA SELECT x AS a, y AS value FROM topicA WITHTIMESTAMP z;
 INSERT INTO bitB SELECT x AS a, y AS value, z as c FROM topicB WITHTIMESTAMP sys_time();
 INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as c FROM topicC WITHTIMESTAMP sys_time()"
}}' | curl -X POST -d @- http://kafkaconnecthost:8083/connectors --header "content-Type:application/json"
```
