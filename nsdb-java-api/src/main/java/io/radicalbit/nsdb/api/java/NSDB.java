/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.client.rpc.GRPCClient;
import io.radicalbit.nsdb.rpc.common.Dimension;
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse;
import io.radicalbit.nsdb.rpc.request.RPCInsert;
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static scala.compat.java8.FutureConverters.toJava;

/**
 * Nsdb connection class.
 * Provides methods to define a bit to be inserted and a query to be executed leveraging a builder pattern.
 * <pre>{@code
 * NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();
 * // to write a bit
 * NSDB.Bit bit = nsdb.db("root").namespace("registry")
 * .bit("people")
 * .value(new java.math.BigDecimal("13"))
 * .dimension("city", "Mouseton")
 * .dimension("gender", "M")
 * .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
 *
 * InsertResult result = nsdb.write(bit).get();
 * //to execute a select query
 * NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").query("select * from people limit 1");
 * QueryResult result = nsdb.executeStatement(statement).get();
 * }</pre>
 */
public class NSDB {

    /**
     * Auxiliar class to specify the Db in the Apis
     */
    public static class Db {
        private String name;

        private Db(String name) {
            this.name = name;
        }

        /**
         * defines the namespace used to build the bit or the query
         *
         * @param namespace the db name
         * @return
         */
        public Namespace namespace(String namespace) {
            return new Namespace(this.name, namespace);
        }
    }

    /**
     * Auxiliar class to specify the Namespace in the Apis
     */
    public static class Namespace {
        private String db;
        private String name;

        private Namespace(String db, String name) {
            this.db = db;
            this.name = name;
        }

        /**
         * defines the Sql statement to be executed
         *
         * @param queryString the db name
         * @return the sql statement to execute
         */
        public SQLStatement query(String queryString) {
            return new SQLStatement(this.db, this.name, queryString);
        }

        /**
         * defines the Bit to be inserted
         *
         * @param metric the db name
         * @return the Bit 
         */
        public Bit bit(String metric) {
            return new Bit(db, name, metric);
        }
    }

    /**
     * Auxiliar class to specify the Sql statement in the Apis
     */
    public static class SQLStatement {
        private String db;
        private String namespace;
        private String sQLStatement;

        private SQLStatement(String db, String namespace, String sqlStatement) {
            this.db = db;
            this.namespace = namespace;
            this.sQLStatement = sqlStatement;
        }
    }

    /**
     * Auxiliar class to specify the Bit in the Apis
     */
    public static class Bit {

        private String db;
        private String namespace;
        private String metric;
        private Long timestamp;
        private RPCInsert.Value value;
        private Map<String, Dimension> dimensions;

        private Bit(String db, String namespace, String metric) {
            this.db = db;
            this.namespace = namespace;
            this.metric = metric;
            this.timestamp = System.currentTimeMillis();
            this.value = ScalaUtils.emptyValue();
            this.dimensions = new HashMap<>();
        }

        /**
         * adds a Long timestamp to the bit
         *
         * @param v the timestamp
         * @return a new instance with `v` as a timestamp
         */
        public Bit timestamp(Long v) {
            this.timestamp = v;
            return this;
        }

        /**
         * adds a Long dimension to the bit
         *
         * @param k the dimension name
         * @param d the Long dimension value
         * @return a new instance with `(k,d)` as a dimension
         */
        public Bit dimension(String k, Long d) {
            dimensions.put(k, ScalaUtils.longDimension(d));
            return this;
        }

        /**
         * adds a Integer dimension to the bit
         *
         * @param k the dimension name
         * @param i the Integer dimension value
         * @return a new instance with `(k,v)` as a dimension
         */
        public Bit dimension(String k, Integer i) {
            dimensions.put(k, ScalaUtils.longDimension(i.longValue()));
            return this;
        }

        /**
         * adds a Double dimension to the bit
         *
         * @param k the dimension name
         * @param d the Double dimension value
         * @return a new instance with `(k,v)` as a dimension
         */
        public Bit dimension(String k, Double d) {
            dimensions.put(k, ScalaUtils.decimalDimension(d));
            return this;
        }

        /**
         * adds a String dimension to the bit
         *
         * @param k the dimension name
         * @param d the String dimension value
         * @return a new instance with `(k,v)` as a dimension
         */
        public Bit dimension(String k, String d) {
            dimensions.put(k, ScalaUtils.stringDimension(d));
            return this;
        }

        /**
         * adds a {@link java.math.BigDecimal} dimension to the bit
         *
         * @param k the dimension name
         * @param d the {@link java.math.BigDecimal} dimension value
         * @return a new instance with `(k,v)` as a dimension
         */
        public Bit dimension(String k, java.math.BigDecimal d) {
            if (d.scale() > 0) return dimension(k, d.doubleValue());
            else return dimension(k, d.longValue());
        }

        /**
         * adds a Long value to the bit
         *
         * @param v the Long value
         * @return a new instance with `v` as the value
         */
        public Bit value(Long v) {
            this.value = ScalaUtils.longValue(v);
            return this;
        }

        /**
         * adds a Integer value to the bit
         *
         * @param v the Integer value
         * @return a new instance with `v` as the value
         */
        public Bit value(Integer v) {
            this.value = ScalaUtils.longValue(v.longValue());
            return this;
        }

        /**
         * adds a Double value to the bit
         *
         * @param v the Double value
         * @return a new instance with `v` as the value
         */
        public Bit value(Double v) {
            this.value = ScalaUtils.decimalValue(v);
            return this;
        }

        /**
         * adds a {@link java.math.BigDecimal} value to the bit
         *
         * @param v the BigDecimal value
         * @return a new instance with `v` as the value
         */
        public Bit value(java.math.BigDecimal v) {
            if (v.scale() > 0)
                return value(v.doubleValue());
            else return value(v.longValue());
        }

    }


    private String host;
    private Integer port;
    /**
     * the inner Grpc client
     */
    private GRPCClient client;

    private NSDB(String host, Integer port) {
        this.host = host;
        this.port = port;
        client = new GRPCClient(this.host, this.port);
    }

    /**
     * Factory method to create a connection to an instance of NSDB.
     * <pre>{@code
     *   CompletableFuture<NSDB> nsdb = NSDB.connect("127.0.0.1", 7817);
     * }</pre>
     *
     * @param host Nsdb host
     * @param port Nsdb port
     */
    public static CompletableFuture<NSDB> connect(String host, Integer port) {
        NSDB conn = new NSDB(host, port);
        return conn.check().toCompletableFuture().thenApplyAsync(r -> conn);
    }

    /**
     * check if a connection is healthy
     */
    public CompletableFuture<HealthCheckResponse> check() {
        return toJava(client.checkConnection()).toCompletableFuture();
    }

    /**
     * defines the db used to build the bit or the query
     *
     * @param name the db name
     * @return
     */
    public Db db(String name) {
        return new Db(name);
    }

    /**
     * execute a {@link SQLStatement}  using the current openend connection
     *
     * @param sqlStatement the {@link SQLStatement} to be executed
     * @return a CompletableFuture of the result of the operation. See {@link QueryResult}
     */

    public CompletableFuture<QueryResult> executeStatement(SQLStatement sqlStatement) {
        SQLRequestStatement sqlStatementRequest = new SQLRequestStatement(sqlStatement.db, sqlStatement.namespace, sqlStatement.sQLStatement);
        return toJava(client.executeSQLStatement(sqlStatementRequest)).toCompletableFuture().thenApply(QueryResult::new);
    }

    /**
     * write a bit into Nsdb using the current openened connection
     *
     * @param bit the bit to be inserted
     * @return a Future containing the result of the operation. See {@link InsertResult}
     */

    public CompletableFuture<InsertResult> write(Bit bit) {
        return toJava(client.write(
                new RPCInsert(bit.db, bit.namespace, bit.metric,
                        bit.timestamp, ScalaUtils.convertMap(bit.dimensions), bit.value))).toCompletableFuture().thenApply(InsertResult::new);
    }


}
