package io.radicalbit.nsdb.api.java;

import io.radicalbit.nsdb.rpc.response.RPCInsertResult;

/**
 * Wraps the Grpc insert bit procedure result into a more friendly class
 */
public class InsertResult {
    boolean completedSuccessfully;
    String errors;

    /**
     * friendly Constructor that builds the objcet from the Grpc raw result
     * @param rpcInsertResult the Grpc result
     */
    InsertResult(RPCInsertResult rpcInsertResult) {
        this.completedSuccessfully = rpcInsertResult.completedSuccessfully();
        this.errors = rpcInsertResult.errors();
    }

    /**
     * @return true if the request is completed successfully, false otherwise
     */
    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    /**
     * @return comma separated errors list, if the request is not successfully completed
     */
    public String getErrors() {
        return errors;
    }
}
