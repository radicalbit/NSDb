# Web APIs
Nsdb exposes HTTP and Websocket APIs to perform sql statements and Nsdb commands. HTTP APIs expose the same functionalities  provided by CLI interface.
Furthermore WS APIs allow to register real-time queries on Nsdb cluster. Every time a Bit fulfilling a query is written , it's published on the corresponding WebSocket.

HTTP APIs implement three main route categories:
- Query API, used to run historical select queries.
- Data APi, used to perform insert statements.
- Command API , allowing commands execution e.g. display of namespaces and metrics, drop statements.

All the above-mentioned Web APIs allow custom security authorization mechanism implementation. To define authorization logic, user must implement `io.radicalbit.nsdb.security.http.NSDBAuthProvider` trait defining authorization behaviour for database, namespace and metric models.
Once the custom authorization provider is set up, its canonical class name must be added to Nsdb conf file under the key `nsdb.security.auth-provider-class`.
By default an `io.radicalbit.nsdb.security.http.EmptyAuthorization ` provider class is plugged in implementing no auth logic.

A secure connection can be set up making use of SSL/TLS protocol enabled in Nsdb cluster configuration. To access a more detailed description see SSL/TLS Documentation.

 Swagger documentation is also available for all the endpoints described in this document.

# Query APIs

Allow data retrieval from Nsdb according to a provided query.

**URL** : `/query`

**Method** : `POST`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on metric, depending on security configuration

**Data params**

Provide `db`, `namespace`, `metric` and the query to be executed, and optionally `from` and `to` timestamp to filter the results.

 Dynamic where conditions can be specified using filter definition.
Filter elements are combined through `AND` operator.

```json
{
    "db": "[string]",
    "namespace": "[string]",
    "metric": "[string]",
    "queryString": "[string]",
    "from": "[optional timestamp in epoch format]",
    "to": "[optional timestamp in epoch format]",
    "filters": "[ optional array of Filter] "
}
```
Filter object is defines as below:

```json
{
    "dimension": "[string]",
    "value": "[string|numerical depending on dimension type]",
    "operator" : "[string which value must me in [=, >, >=, <, <=, like]]"
}
```

**Data examples**

`From` and `To` fields are optionals.

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "people",
    "queryString": "select * from people limit 100",
    "filters": [{ "dimension": "dimName1",
                  "value" : "value",
                  "operator": "like" },
                { "dimension": "dimName2",
                  "value" : 1,
                  "operator": ">" }
                ]
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

```json
{
    "db": "db",
    "namespace": "namespace",
    "metric": "people",
    "queryString": "select * from people limit 100",
    "from": 0,
    "to": 100000,
    "filters": [{ "dimension": "dimName1",
                  "value" : "value",
                  "operator": "=" },
                { "dimension": "dimName2",
                  "value" : 1,
                  "operator": "=" }
                ]
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

**Condition** : If metric does not exist

**Code** : `404 NOT FOUND`

### Or

**Condition** : If a generic error occurs.

**Code** : `500 INTERNAL SERVER ERROR`

**Content** : `Error message`

# Data APIs

This API allows bit insertion into Nsdb's metric. Bit description and metric information must be defined in request body.

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

# Commands APis
Commands APis map some functionalities already implemented in Nsdb Command Line Interface. They consist in a set of statements aimed to retrieve information about namespace and metric structure and on the other hand to perform drop action on the latter.
## Show namespaces
`Show namespaces` command retrieve the list of namespaces belonging to the specified database.

**URL** : `/commands/<database_name>/namespaces`

**Method** : `GET`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on selected database, depending on security configuration

**Data params**: Provide database name in url

### Example
```
http://<host>:<port>/commands/database/namespaces
```
### Success Response
**Condition:**  `database` in url path exists and user has authorization access to the latter

**Code:** `200 OK`

**Content example**
```json
{
  "namespaces": [
    "namespace_name1",
    "namespace_name2"
  ]
}
```
### Error Responses

**Condition** : Server error

**Code** : `500 Internal Server Error`

**Content** : `Error message`

## Show metrics
`Show metrics` command retrieves the list of metrics belonging to the specified namespace.

**URL** : `/commands/<database_name>/<namespace_name>/metrics`

**Method** : `GET`

**Auth required** : Depending on security configuration.

**Permissions required** : Read permission on selected namespace, depending on security configuration.

**Data params**: Provide database name and namespace name in url.

### Success Response
**Condition:**  `database` and `namespace` in url path exist and user has authorization access to the latter.

**Code:** `200 OK`

**Content example**
```json
{
  "metrics": [
    "metric_name1",
    "metric_name2"
  ]
}
```
### Error Response

**Condition** : Server error

**Code** : `500 Internal Server Error`

**Content** : `Error message`


## Describe metric
`Describe metric` command retrieves the schema descriptor of a specific metric.

**URL** : `/commands/<database_name>/<namespace_name>/<metric_name>`

**Method** : `GET`

**Auth required** : Depending on security configuration

**Permissions required** : Read permission on selected metric, depending on security configuration

**Data params**: Provide database, namespace  and metric name in url

### Success Response
**Condition:**  `database`,`namespace` and `metric` in url path exist and user has authorization access to the latter.

**Code:** `200 OK`

**Content example**
```json
{
  "fields": [
      { "name" : "dimension_1",
        "type" : "VARCHAR"
      },
      { "name" : "dimension_2",
        "type" : "INTEGER"
      }
  ]
}
```
### Error Responses

**Condition** : Server error

**Code** : `500 Internal Server Error`

**Content** : `Error message`

#### Or

**Condition** : Metric not found

**Code** : `404 Not Found`

## Drop namespace

`Drop Namespace` command drops selected namespace and all belonging metrics.

**URL** : `/commands/<database_name>/<namespace_name>`

**Method** : `DELETE`

**Auth required** : Depending on security configuration.

**Permissions required** : Write permission on selected namespace, depending on security configuration.

**Data params**: Provide database and namespace name in url.

### Success Response
**Condition:**  `database` and `namespace`  in url path exist and user has authorization access to the latter.

**Code:** `200 OK`

### Error Responses

**Condition** : Server error

**Code** : `500 Internal Server Error`

**Content** : `Error message`

## Drop metric

`Drop Metric` command drops selected metric, deleting schema and related data.

**URL** : `/commands/<database_name>/<namespace_name>/<metric_name>`

**Method** : `DELETE`

**Auth required** : Depending on security configuration.

**Permissions required** : Write permission on selected metric, depending on security configuration.

**Data params**: Provide database,namespace and metric name in url.

### Success Response
**Condition:**  `database`, `namespace`, `metric`  in url path exist and user has authorization access to the latter.

**Code:** `200 OK`

### Error Responses

**Condition** : Server error

**Code** : `500 Internal Server Error`

**Content** : `Error message`
