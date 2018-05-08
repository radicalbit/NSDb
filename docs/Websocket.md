# WebSocket APIs

Opening a websocket using this API allows to subscribe to a query and listen to data updates.

Subscribe to a query and listen to data updates.
Every inserted event will be checked against every registered query. If the event fulfills the query, it will be published to each subscribed WebSocket.

**URL** : `/ws-stream?refresh_period=200&retention_size=10`

**Method** : `ws`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on metric, depending on security configuration

**Query String params** :
Optionally, user can specify two parameters: `refresh_period` and `retention_size`.

The `refresh_period` sets the minimum period between two records belonging to the same subscribed query are sent to the browser. Due to the possibility to flood the web UI with a not manageable amount of data, `refresh_period` has a default value fixed to 100 milliseconds by Nsdb's configuration. User can define a value greater than the default one but not lower.
Default value is defined at key `nsdb.websocket.refresh-period` in cluster configuration.

The  `retention_size` number of retained messages for each query. It's not mandatory, the default value is defined in configuration at key `nsdb.websocket.retention-size`.

**Example:**
`/ws-stream?refresh_period=200&retention_size=10`

**Data params**:
In order to subscribe a query, after connection is being opened, user has to send a POST request providing `db`, `namespace`, `metric` and the query to be subscribed to.

```json
{
    "db": "[string]",
    "namespace": "[string]",
    "metric": "[string]",
    "queryString" : "[string]",
    "filters": "[optional array of Filter]"
}
```

**Data example**

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "metric",
    "queryString" : "select * from metric limit 1",
    "filters": [{ "dimension": "dimName1",
                  "value" : "value",
                  "operator": "=" },
                { "dimension": "dimName2",
                  "value" : 1,
                  "operator": ">=" }]
}
```

## Success Response

**Condition** : If the query provided is valid, it will be executed and the results will be published to the socket. In the response there will be also the querystring provided and the quid, an internal query identifier.

**Content example**

```json
{
    "queryString":"select * from metric limit 1",
    "quid": "3bb06ef5-e09c-424d-a347-14a895d0f1a9",
    "records": [
        {
            "timestamp": 100000,
            "value": 1,
            "dimensions": {
                "key": "Key1"
            }
        },
        {
            "timestamp": 100000,
            "value": 2,
            "dimensions": {
                "key": "Key2"
            }
        }
    ]
}
```

**Condition** : Everytime a new record that satisfies the query is inserted, it will be pushed to the socket

**Content example**

```json
{
    "quid":"3bb06ef5-e09c-424d-a347-14a895d0f1a9",
    "metric":"metric",
    "records":[
        {
            "timestamp":100000,
            "value": 15.0,
            "dimensions": {
                "key": "Key1"
            }
        }
    ]
}
```

## Error Responses

**Condition** : If the query provided is invalid

**Content example** :
```json
{
    "db": "db",
    "namespace":"namespace",
    "queryString":"select * from metric limit 1",
    "reason":"reason"
}
```
