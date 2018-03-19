package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.rpc.common.Bit;

import java.util.List;

/**
 * Wraps the Grpc execute query procedure sesult into a more friendly class
 */
public class QueryResult {
    private String db;
    private String namespace;
    private String metric;
    private boolean completedSuccessfully;
    private String reason;
    private List<Bit> records;

    /**
     * friendly Constructor that builds the objcet from the Grpc raw result
     * @param rpcStatement the Grpc result
     */
    QueryResult(io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse rpcStatement) {
        this.db = rpcStatement.db();
        this.namespace = rpcStatement.namespace();
        this.metric = rpcStatement.metric();
        this.completedSuccessfully = rpcStatement.completedSuccessfully();
        this.reason = rpcStatement.reason();
        this.records = scala.collection.JavaConversions.seqAsJavaList(rpcStatement.records());
    }

    /**
     * @return the query db
     */
    public String getDb() {
        return db;
    }

    /**
     * @return the query namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return the query metric
     */
    public String getMetric() {
        return metric;
    }

    /**
     * @return true if the request is completed successfully, false otherwise
     */
    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    /**
     * @return the error
     */
    public String getReason() {
        return reason;
    }

    /**
     * @return the records that fulfill the query provided
     */
    public List<Bit> getRecords() {
        return records;
    }
}
