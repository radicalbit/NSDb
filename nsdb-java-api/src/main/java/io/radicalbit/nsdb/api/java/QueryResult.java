package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.rpc.common.Bit;

import java.util.List;

public class QueryResult {
    private String db;
    private String namespace;
    private String metric;
    private boolean completedSuccessfully;
    private String reason;
    private List<Bit> records;

    QueryResult(io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse rpcStatement) {
        this.db = rpcStatement.db();
        this.namespace = rpcStatement.namespace();
        this.metric = rpcStatement.metric();
        this.completedSuccessfully = rpcStatement.completedSuccessfully();
        this.reason = rpcStatement.reason();
        this.records = scala.collection.JavaConversions.seqAsJavaList(rpcStatement.records());
    }

    public String getDb() {
        return db;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getMetric() {
        return metric;
    }

    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    public String getReason() {
        return reason;
    }

    public List<Bit> getRecords() {
        return records;
    }
}
