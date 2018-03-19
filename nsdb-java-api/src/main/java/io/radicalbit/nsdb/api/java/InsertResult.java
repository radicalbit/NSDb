package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.rpc.response.RPCInsertResult;

public class InsertResult {
    boolean completedSuccessfully;
    String errors;

    InsertResult(RPCInsertResult rpcInsertResult) {
        this.completedSuccessfully = rpcInsertResult.completedSuccessfully();
        this.errors = rpcInsertResult.errors();
    }

    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    public String getErrors() {
        return errors;
    }
}
