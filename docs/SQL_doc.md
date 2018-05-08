# Supported SQL Dialect Reference
To interact with NSDb, a custom SQL like query language must be used. It has been designed to suit NSDb metrics structure and in the meantime to feel familiar with standard SQL.
Since the nature of NSDb is different than a classic SQL database, its dialect is conceived to handle time-series data accordingly.

## SQL Statements
Similarly to SQL-like databases, NSDb allows 3 main DML statements:
- `SELECT` Statements - used to fetch data
- `INSERT` Statements - used to insert data
- `DELETE` Statements - used to delete data

Unlike standard SQL, NSDb does not support `UPDATE` statement. The main reason of this is the database time series nature itself; it's extremely unlikely that a time series record has to be updated at a later time.

As in SQL, some clauses composing the above-mentioned constructs are shared between these categories e.g `WHERE` clause.

>NOTE: NSDb SQL parser is case-insensitive, so SQL keywords can be both lower-case and upper-case. In this documentation all code example are uppercase for clarity purpose.

## The Select Statement
The `SELECT` statement queries measurements from a specific metric.
`SELECT` statements are used to perform data exploration and to subscribe historical or real-time queries.

### Simple Syntax

```sql
SELECT <dimension_name> [,<dimension_name>, ... ]
FROM <metric_name>
[ WHERE <expression> ]
[ GROUP BY <dimension_name> ]
[ ORDER BY <dimension_name> [DESC] ]
[ LIMIT v ]
```

`SELECT` statements requires a `SELECT` clause and a `FROM` clause.

Furthermore `WHERE` condition can be specified to retrieve bits that meet the  `WHERE` clause condition. See [WHERE clause](#WHERE clause)
Retrieved data may be ordered using one of the projected dimension.See [ORDER BY clause](#WHERE clause)

### SELECT clause
The `SELECT` clause defines which bit's dimensions are projected.

>NOTE: Bit's `value` and `timestamp` are always included in projections.

#### Syntax

`SELECT ` statements supports different formats:

`SELECT * `  Returns all dimension in the selected metric.
```sql
SELECT  * FROM metric
```
` SELECT <dimension_name> ` applies projection on the selected dimension.
```sql
SELECT  value FROM metric
```
`SELECT <dimension_name>, <dimension_name>+` applies projection on the selected dimensions.
```sql
SELECT  value, dimension FROM metric
```
`COUNT` operators can be applied in a `SELECT` clause , returning the number of bits in a metric if no `WHERE` clauses are defined.
```sql
SELECT  COUNT(*) FROM metric
```


#### DISTINCT operator
Defining the `SELECT` clause users can apply a `DISTINCT` operator on the projected dimension. This operator allows to return distinct values for the dimension on which is applied.

Usage example:

```sql
SELECT  DISTINCT dimension FROM metric
```
> NOTE: projecting only a **single** dimension is allowed when `DISTINCT` operator is used-

### WHERE clause
The `WHERE` clause allows to define a boolean condition, based on which data are filtered.
```sql
SELECT <dimension_name>[,<dimension_name>, [...]]
FROM <metric_name>
WHERE <expression> [(AND|OR) <expression> [...]]
```
The WHERE clause supports comparisons against VARCHAR, INT, DECIMAL, BIGINT dimensions datatypes.

Supported numerical string operators:
- `=`   equal to
- `<>` not equal to
- `!=` not equal to
- `>`   greater than
- `>=` greater than or equal to
- `<`   less than
- `<=` less than or equal to
- `IN`    between lower and upper bounds

```sql
SELECT dimension FROM metric WHERE numerical_dimension = 1 AND another_numerical_dimension = 2
```
Supported string operators:
- `=`   equal to
- `LIKE` like operator

```sql
SELECT dimension FROM metric WHERE string_dimension = myStringValue AND another_string_dimension LIKE startWith$
```
#### LIKE operator
The `LIKE` operator is used to express `WHERE` conditions in which user have to match part of the complete string value.
Accordingly to common databases standard the character `$` is used as placeholder.
```sql
SELECT dimension FROM metric WHERE string_dimension LIKE $endWith AND another_string_dimension LIKE startWith$
```
#### Time operators
Since NSDb is a time-series database, specific operators handling `timestamp` field are defined. However since `timestamp` is a numerical field the above-mentioned numerical operators are still valid.
In addition to the latter, time operators are implemented allowing users to express time dependent expression in a friendly manner.
Ad ad hoc operator `NOW` is available to express the actual timestamp in milliseconds.
Simple arithmetic operations can by applied on this value:
- `NOW +|- <X>h ` returns the actual timestamp plus|minus  `X` hours
- `NOW +|- <X>m ` returns the actual timestamp plus|minus  `X` minutes
- `NOW +|- <X>s ` returns the actual timestamp plus|minus  `X` seconds

#### Nullability operators
According to the dynamic nature of NSDb schemas, dimensions can be omitted during data insertions, therefore Null values may be filtered inclusively or exclusively using dedicated operators:
- `WHERE <dimension_name> IS NULL` creates an expression filtering data without values for the specified dimension.
- `WHERE <dimension_name> IS NOT NULL ` creates an expression filtering data with values for the specified dimension.

Examples:
```sql
SELECT dimension FROM metric WHERE dimension IS NULL
```
```sql
SELECT dimension FROM metric WHERE dimension IS NOT NULL
```
### GROUP BY clause
The `GROUP BY ` clause groups query result using a specified dimension.
```sql
SELECT <dimension_name>[,<dimension_name>, [...]]
FROM <metric_name>
[WHERE <expression>]
[GROUP BY <dimension_name>]
```
If the query includes a `WHERE`  clause the `GROUP BY` clause must appear after the `WHERE` clause.
> NOTE: `GROUP BY` clause accepts a **single** dimension/field on which apply the grouping

When defining a `GROUP BY` clause **value functions** can be defined in `SELECT` clause.

#### Value Functions
Value functions are arithmetic operations applied on **value field** of bits retrieved by a `SELECT` statement with a `GROUP BY` clause.
Basic arithmetic functions are available:
- `MIN` retrieve min value for each group.
```sql
SELECT MIN(value) FROM metric GROUP BY dimension_name
```
- `MAX` retrieve max value for each group.
```sql
SELECT MAX(value) FROM metric GROUP BY dimension_name
```
- `SUM` retrieve value sum for each group.
```sql
SELECT SUM(value) FROM metric GROUP BY dimension_name
```

### ORDER BY clause
Query results ordering can be applied defining an `ORDER BY` . The `ORDER BY` clause must appear before `LIMIT` clause and not before `WHERE` and `GROUP BY`  clauses.

```sql
SELECT <select_clause>
FROM <metric_name>
[WHERE <where_clause>]
[GROUP BY <group_by_clause>]
[ORDER BY <dimension_name>[DESC]]
```

Two ordering are defined:
- Ascending, the default one if `ORDER BY` clause is defined.
- Descending, in that case `DESC` keyword must be present in `ORDER_BY` clause.

```sql
SELECT dimension1, dimension2 FROM metric ORDER BY value DESC
```

```sql
SELECT dimension1, dimension2 FROM metric WHERE dimension1 >= 1 ORDER BY dimension2 DESC
```

### LIMIT clause
The `LIMIT` clause allows users to define the maximum number of bits to be retrieved. `LIMIT` clause definition is not mandatory, but strongly recommended in order to speed up response time.

```sql
SELECT <select_clause>
FROM <metric_name>
[WHERE <where_clause>]
[GROUP BY <group_by_clause>]
[ORDER BY <dimension_name>[DESC]]
[LIMIT n]
```

Using `limit` clause in combination with `order` can be very useful to retrieve oldest or yougest records of a time series.

For example, the query below returns the 10 youngest records

```sql
SELECT * FROM metric WHERE dimension1 >= 1 ORDER BY timestamp DESC LIMIT 10
```

while the following

```sql
SELECT * FROM metric WHERE dimension1 >= 1 ORDER BY timestamp LIMIT 10
```

the 10 oldest ones.

## The Insert Statement

NSDb allows bit insertion making use of  Insert statement, whose syntax is similar to standard SQL insert.
By the way there are some small differences meant in time-series concept that introduces value and timestamp fields.

### Simple Syntax

 ```sql
 INSERT INTO <metric_name> [TS=<timestamp_value>] DIM ( [<dimension_name> = <dimension_value>*] ) VAL = <value>
 ```
 The above mentioned syntax inserts a single Bit whose dimensions tuples  `(name, value)` are declared after `DIM` clause. Value field is assigned using `VAL` clause , that accepts a numerical value.
 Timestamp index definition is not mandatory, but it can be defined using `TS` clause.

 > NOTE: , if  `TS` clause's value is not defined, it is assigned the epoch-timestamp of the istant the insertion is performed .

### Examples

```sql
INSERT INTO metric DIM ( dimension_1 = 1, dimension_2 = myStringValue ) VAL = 1
```
```sql
INSERT INTO metric DIM ( dimension_1 = 1, dimension_2 = 'my String Value' ) VAL = 1
```
> NOTE: , user can define VARCHAR dimensions' values wrapping them inside `' '`, this notation is mandatory in case of strings with spaces.

```sql
INSERT INTO metric TS = 1522232017 DIM ( dimension_1 = 1, dimension_2 = 'myStringValue' ) VAL = 1.5
```
## Delete Statement
Delete statement allows Bits deletion defining `WHERE` clause to express the condition based on deletion is performed.
### Simple Syntax

```sql
DELETE FROM <metric_name> WHERE <where_clause>
```

> NOTE: :`WHERE` clause must be qualified to express deletion condition. To delete all bits belonging to a specific metric user must use `DELETE METRIC` NSDb command or the tricky query `DELETE FROM <metric_name> WHERE timestamp > 0` which works assuming that the metric describes a non-relativistic physics phenomenon.

### Examples
```sql
DELETE FROM metric WHERE timestamp IN (2,4)
```
```sql
DELETE FROM metric WHERE NOT timestamp >= 2 OR timestamp < 4
```
