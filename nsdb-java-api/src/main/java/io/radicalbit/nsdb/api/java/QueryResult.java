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

import io.radicalbit.nsdb.rpc.common.Bit;

import java.util.List;

/**
 * Wraps the Grpc execute query procedure result into a more friendly class
 */
public class QueryResult {
    private String db;
    private String namespace;
    private String metric;
    private boolean completedSuccessfully;
    private String reason;
    private List<Bit> records;

    /**
     * friendly Constructor that builds the object from the Grpc raw result
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
