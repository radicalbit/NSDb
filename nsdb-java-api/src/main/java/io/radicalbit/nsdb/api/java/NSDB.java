/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import io.grpc.stub.StreamObserver;
import io.radicalbit.nsdb.client.rpc.GRPCClient;
import io.radicalbit.nsdb.client.rpc.TokenApplier;
import io.radicalbit.nsdb.client.rpc.TokenAppliers;
import io.radicalbit.nsdb.rpc.common.Dimension;
import io.radicalbit.nsdb.rpc.common.Tag;
import io.radicalbit.nsdb.rpc.init.InitMetricRequest;
import io.radicalbit.nsdb.rpc.request.RPCInsert;
import io.radicalbit.nsdb.rpc.requestCommand.DescribeMetric;
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement;
import io.radicalbit.nsdb.rpc.streaming.SQLStreamingResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
        private final String name;

        private Db(String name) {
            this.name = name;
        }

        /**
         * defines the namespace used to build the namespace
         *
         * @param namespace the db name
         */
        public Namespace namespace(String namespace) {
            return new Namespace(this.name, namespace);
        }
    }

    /**
     * Auxiliary class to specify the Namespace in the Apis
     */
    public static class Namespace {
        private final String db;
        private final String namespace;

        private Namespace(String db, String namespace) {
            this.db = db;
            this.namespace = namespace;
        }

        /**
         * defines the Metric to be inserted
         *
         * @param metric the db name
         * @return the Bit
         */
        public Metric metric(String metric) {
            return new Metric(this.db, this.namespace, metric);
        }

        public Bit bit(String metric) {
            return new Bit(this.db, this.namespace, metric);
        }
    }

    public static class Metric {
        private final String db;
        private final String namespace;
        private final String metric;

        private Metric(String db, String namespace, String metric) {
            this.db = db;
            this.namespace = namespace;
            this.metric = metric;
        }

        /**
         * defines the Sql statement to be executed
         *
         * @param queryString the db name
         * @return the sql statement to execute
         */
        public SQLStatement query(String queryString) {
            return new SQLStatement(this.db, this.namespace, this.metric, queryString);
        }
    }

    /**
     * Auxiliar class to specify the Sql statement in the Apis
     */
    public static class SQLStatement {
        private final String db;
        private final String namespace;
        private final String metric;
        private final String sQLStatement;

        private SQLStatement(String db, String namespace, String metric, String sqlStatement) {
            this.db = db;
            this.namespace = namespace;
            this.metric = metric;
            this.sQLStatement = sqlStatement;
        }
    }

    /**
     * Auxiliar class to specify the Bit in the Apis
     */
    public static class Bit {

        private final String db;
        private final String namespace;
        private final String metric;
        private Long timestamp;
        private RPCInsert.Value value;
        private final Map<String, Dimension> dimensions;
        private final Map<String, Tag> tags;

        private Bit(String db, String namespace, String metric) {
            this.db = db;
            this.namespace = namespace;
            this.metric = metric;
            this.timestamp = System.currentTimeMillis();
            this.value = ScalaUtils.emptyValue();
            this.dimensions = new HashMap<>();
            this.tags = new HashMap<>();
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
         * adds a Long tag to the bit
         *
         * @param k the tag name
         * @param d the Long tag value
         * @return a new instance with `(k,d)` as a tag
         */
        public Bit tag(String k, Long d) {
            tags.put(k, ScalaUtils.longTag(d));
            return this;
        }

        /**
         * adds a Integer tag to the bit
         *
         * @param k the tag name
         * @param i the Integer tag value
         * @return a new instance with `(k,v)` as a tag
         */
        public Bit tag(String k, Integer i) {
            tags.put(k, ScalaUtils.longTag(i.longValue()));
            return this;
        }

        /**
         * adds a Double tag to the bit
         *
         * @param k the tag name
         * @param d the Double tag value
         * @return a new instance with `(k,v)` as a tag
         */
        public Bit tag(String k, Double d) {
            tags.put(k, ScalaUtils.decimalTag(d));
            return this;
        }

        /**
         * adds a String tag to the bit
         *
         * @param k the tag name
         * @param d the String tag value
         * @return a new instance with `(k,v)` as a tag
         */
        public Bit tag(String k, String d) {
            tags.put(k, ScalaUtils.stringTag(d));
            return this;
        }

        /**
         * adds a {@link java.math.BigDecimal} tag to the bit
         *
         * @param k the tag name
         * @param d the {@link java.math.BigDecimal} tag value
         * @return a new instance with `(k,v)` as a tag
         */
        public Bit tag(String k, java.math.BigDecimal d) {
            if (d.scale() > 0) return tag(k, d.doubleValue());
            else return tag(k, d.longValue());
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

        /**
         * Builds a MetricInfo from a Bit by specifying the shardinterval
         * @param interval shard interval according to the Duration semantic (1d, 2h, 1m etc.)
         * @return the MetricInfo with the given shard interval
         */
        public MetricInfo shardInterval(String interval) {
            return new MetricInfo(db, namespace, metric, interval, "");
        }

        /**
         * Builds a MetricInfo from a Bit by specifying the retention
         * @param retention metric retention according to the Duration semantic (1d, 2h, 1m etc.)
         * @return the MetricInfo with the given shard interval
         */
        public MetricInfo retention(String retention) {
            return new MetricInfo(db, namespace, metric, "", retention);
        }

    }

    public static class MetricInfo {

        private final String db;
        private final String namespace;
        private final String metric;
        private String shardInterval;
        private String retention;

        private MetricInfo(String db, String namespace, String metric, String shardInterval, String retention) {
            this.db = db;
            this.namespace = namespace;
            this.metric = metric;
            this.shardInterval = shardInterval;
        }

        /**
         * Adds a shard interval the existing instance.
         * @param interval shard interval according to the Duration semantic (1d, 2h, 1m etc.)
         * @return the MetricInfo with the given shard interval
         */
        public MetricInfo shardInterval(String interval) {
            this.shardInterval = interval;
            return this;
        }

        /**
         * Adds a retention to the existing instance.
         * @param retention metric retention according to the Duration semantic (1d, 2h, 1m etc.)
         * @return the MetricInfo with the given shard interval
         */
        public MetricInfo retention(String retention) {
            this.retention = retention;
            return this;
        }

    }


    /**
     * the inner {@link GRPCClient}
     */
    private final GRPCClient client;

    private NSDB(String host, Integer port) {
        this.client = new GRPCClient(host, port);
    }

    private NSDB(String host, Integer port, TokenApplier tokenApplier) {
        this.client = new GRPCClient(host, port, tokenApplier);
    }

    /**
     * Creates a NSDb Connection with a Jwt token.
     * @param token the Jwt token that will be provided in the low level client.
     */
    public NSDB withJwtToken(String token) {
        return new NSDB(this.client.host(), this.client.port(), TokenAppliers.JWT(token));
    }

    /**
     * Creates a NSDb Connection with a Custom token.
     * @param tokenName name of the token.
     * @param tokenValue value of the token.
     */
    public NSDB withCustomToken(String tokenName, String tokenValue) {
        return new NSDB(this.client.host(), this.client.port(), TokenAppliers.Custom(tokenName, tokenValue));
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
        return CompletableFuture.supplyAsync(() -> new NSDB(host, port));
    }

    /**
     * check if a connection is healthy
     * @return the connection instance.
     */
    public CompletableFuture<NSDB> check() {
        return toJava(client.checkConnection()).toCompletableFuture().thenApply(response -> this);
    }

    /**
     * defines the db used to build the bit or the query
     *
     * @param name the db name
     */
    public Db db(String name) {
        return new Db(name);
    }

    /**
     * execute a {@link SQLStatement}  using the current opened connection
     *
     * @param sqlStatement the {@link SQLStatement} to be executed
     * @return a CompletableFuture of the result of the operation. See {@link QueryResult}
     */
    public CompletableFuture<QueryResult> executeStatement(SQLStatement sqlStatement) {
        SQLRequestStatement sqlStatementRequest = new SQLRequestStatement(sqlStatement.db, sqlStatement.namespace, sqlStatement.metric, sqlStatement.sQLStatement, scalapb.UnknownFieldSet.empty());
        return toJava(client.executeSQLStatement(sqlStatementRequest)).toCompletableFuture().thenApply(QueryResult::new);
    }

    /**
     * subscribe to a {@link SQLStatement} using the current opened connection.
     * This works similarly to the web-socket publish subscribe.
     *
     * @see <a href="https://nsdb.io/PublishSubscribe">nsdb.io</a> for more details.
     *
     * @param sqlStatement the {@link SQLStatement} to be executed
     * @param callback a callback that receives all the {@link SQLStreamingResponse} records
     */
    public void subscribe(SQLStatement sqlStatement, Consumer<SQLStreamingResponse> callback) {
        SQLRequestStatement sqlStatementRequest = new SQLRequestStatement(sqlStatement.db, sqlStatement.namespace, sqlStatement.metric, sqlStatement.sQLStatement, scalapb.UnknownFieldSet.empty());
        client.subscribe(sqlStatementRequest, new StreamObserver<SQLStreamingResponse>() {
            @Override
            public void onNext(SQLStreamingResponse value) {
                callback.accept(value);
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {}
        });
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
                        bit.timestamp, bit.value, ScalaUtils.convertMap(bit.dimensions), ScalaUtils.convertMap(bit.tags), scalapb.UnknownFieldSet.empty()))).toCompletableFuture().thenApply(InsertResult::new);
    }


    public CompletableFuture<InitMetricResult> initMetric(MetricInfo metricInfo) {
        return toJava(client.initMetric(
                new InitMetricRequest(metricInfo.db, metricInfo.namespace, metricInfo.metric, metricInfo.shardInterval, metricInfo.retention, scalapb.UnknownFieldSet.empty()))).toCompletableFuture().thenApply(InitMetricResult::new);
    }

    public CompletableFuture<DescribeMetricResult> describe(Bit bit) {
        return toJava(client.describeMetric(
                new DescribeMetric(bit.db, bit.namespace, bit.metric, scalapb.UnknownFieldSet.empty()))).toCompletableFuture().thenApply(DescribeMetricResult::new);
    }


}
