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

public class NSDB {

    public static class Db {
        private String name;

        private Db(String name) {
            this.name = name;
        }

        public Namespace namespace(String namespace) {
            return new Namespace(this.name, namespace);
        }
    }


    public static class Namespace {
        private String db;
        private String name;

        private Namespace(String db, String name) {
            this.db = db;
            this.name = name;
        }

        public SQLStatement query(String queryString) {
            return new SQLStatement(this.db, this.name, queryString);
        }

        public Bit bit(String metric) {
            return new Bit(db, name, metric);
        }
    }

    public static class SQLStatement {
        private String db;
        private String namespace;
        private String sQLStatement;

        private SQLStatement(String db, String namespace, String sqlStatement) {
            this.db = db;
            this.namespace = namespace;
            this.sQLStatement = sqlStatement;
        }

        public String getDb() {
            return db;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getsQLStatement() {
            return sQLStatement;
        }
    }

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

        public Bit timestamp(Long v) {
            this.timestamp = v;
            return this;
        }

        public Bit dimension(String k, Long d) {
            dimensions.put(k, ScalaUtils.longDimension(d));
            return this;
        }

        public Bit dimension(String k, Integer i) {
            dimensions.put(k, ScalaUtils.longDimension(i.longValue()));
            return this;
        }

        public Bit dimension(String k, Double d) {
            dimensions.put(k, ScalaUtils.decimalDimension(d));
            return this;
        }

        public Bit dimension(String k, String d) {
            dimensions.put(k, ScalaUtils.stringDimension(d));
            return this;
        }

        public Bit dimension(String k, java.math.BigDecimal d) {
            if (d.scale() > 0) return dimension(k, d.doubleValue());
            else return dimension(k, d.longValue());
        }

        public Bit value(Long v) {
            this.value = ScalaUtils.longValue(v);
            return this;
        }

        public Bit value(Integer v) {
            this.value = ScalaUtils.longValue(v.longValue());
            return this;
        }

        public Bit value(Double v) {
            this.value = ScalaUtils.decimalValue(v);
            return this;
        }

        public Bit value(java.math.BigDecimal v) {
            if (v.scale() > 0)
                return value(v.doubleValue());
            else return value(v.longValue());
        }

    }

    private String host;
    private Integer port;
    private GRPCClient client;

    private NSDB(String host, Integer port) {
        this.host = host;
        this.port = port;
        client = new GRPCClient(this.host, this.port);
    }

    public static CompletableFuture<NSDB> connect(String host, Integer port) {
        NSDB conn = new NSDB(host, port);
        return conn.check().toCompletableFuture().thenApplyAsync(r -> conn);
    }

    public CompletableFuture<HealthCheckResponse> check() {
        return toJava(client.checkConnection()).toCompletableFuture();
    }

    public Db db(String name) {
        return new Db(name);
    }

    public CompletableFuture<QueryResult> executeStatement(SQLStatement sqlStatement) {
        SQLRequestStatement sqlStatementRequest = new SQLRequestStatement(sqlStatement.db, sqlStatement.namespace, sqlStatement.sQLStatement);
        return toJava(client.executeSQLStatement(sqlStatementRequest)).toCompletableFuture().thenApply(QueryResult::new);
    }

    public CompletableFuture<InsertResult> write(Bit bit) {
        return toJava(client.write(
                new RPCInsert(bit.db, bit.namespace, bit.metric,
                        bit.timestamp, ScalaUtils.convertMap(bit.dimensions), bit.value))).toCompletableFuture().thenApply(InsertResult::new);
    }


}
