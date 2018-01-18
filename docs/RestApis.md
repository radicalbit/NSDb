# Query api

Retrieve data from nsdb according to a query provided

**URL** : `/query`

**Method** : `POST`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on metric, depending on security configuration

**Data params**

Provide `db`, `namespace`, `metric` and the query to be executed, and optionally `from` and `to` timestamp to filter the results.

```json
{
    "db": "[string]",
    "namespace": "[string]",
    "metric": "[string]",
    "queryString": "[string]",
    "from": "[optional timestamp in epoch format]",
    "to": "[optional timestampin epoch format]"
}
```

**Data example** 

From and To fields are optionals.

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "people",
    "queryString": "select * from people limit 100"
}
```

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "people",
    "queryString": "select * from people limit 100",
    "from": 0,
    "to": 100000
}
```

## Success Response

**Condition** : If the query provided is valid an array of records will be returned

**Code** : `200 OK`

**Content example**

```json
{
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

## Error Responses

**Condition** : If query is invalid.

**Code** : `400 BAD REQUEST`

**Content** : `Error message`

### Or

**Condition** : If a generic error occurs.

**Code** : `500 INTERNAL SERVER ERROR`

**Content** : `Error message`


# Data api

Insert data

**URL** : `/data`

**Method** : `POST`

**Auth required** : Depending on security configuration

**Permissions required** : Write permission on metric, depending on security configuration

**Data params**

Provide `db`, `namespace`, `metric` and the `bit` to be inserted.

```json
{
    "db": "[string]",
    "namespace": "[string]",
    "metric": "[string]",
    "bit": {
        "timestamp": "[epoch timestamp]",
                "value": "[numeric value]",
                "dimensions": {
                    "dim1" : "[numeric or string value]",
                    "dim2" : "[numeric or string value]"
                }
            }
}
```

**Data example** 

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "metric",
    "bit": {
        "timestamp": 100000,
                "value": 0.5,
                "dimensions": {
                    "dim1" : 25,
                    "dim2" : "dim2"
                }
            }
}
```

## Success Response

**Condition** : If the bit provided is valid a OK response will be returned

**Code** : `200 OK`


## Error Responses

**Condition** : If bit is invalid.

**Code** : `400 BAD REQUEST`

**Content** : `Error message`

### Or

**Condition** : If a generic error occurs.

**Code** : `500 INTERNAL SERVER ERROR`

**Content** : `Error message`

# Websocket api

Subscribe to a query and listen to data updates

**URL** : `/ws-stream`

**Method** : `ws`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on metric, depending on security configuration

**Data params**

Provide `db`, `namespace`, `metric` and the query to be subscribed to

```json
{
    "db": "[string]",
    "namespace": "[string]",
    "metric": "[string]",
    "queryString" : "[string]"
}
```

**Data example** 

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "metric",
    "queryString" : "select * from metric limit 1"
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