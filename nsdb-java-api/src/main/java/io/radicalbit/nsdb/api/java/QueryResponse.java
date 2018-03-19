package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.rpc.common.Bit;

import java.util.List;

public class QueryResponse {
    private String db;
    private String namespace;
    private String metric;
    private boolean completedSuccessfully;
    private String reason;
    private List<Bit> records;

    public QueryResponse(io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse rpcStatement) {
        this.db = rpcStatement.db();
        this.namespace = rpcStatement.namespace();
        this.metric = rpcStatement.metric();
        this.completedSuccessfully = rpcStatement.completedSuccessfully();
        this.reason = rpcStatement.reason();
        this.records = scala.collection.JavaConversions.seqAsJavaList(rpcStatement.records());
    }
}
