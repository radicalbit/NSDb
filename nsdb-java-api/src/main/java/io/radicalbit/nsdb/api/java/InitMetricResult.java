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

import io.radicalbit.nsdb.rpc.init.InitMetricResponse;

/**
 * Wraps the Grpc init metric procedure result into a more friendly class
 */
public class InitMetricResult {
    String db;
    String namespace;
    String metric;
    boolean completedSuccessfully;
    String errorMsg;

    /**
     * friendly Constructor that builds the object from the Grpc raw result
     * @param initMetricResponse the Grpc result
     */
    InitMetricResult(InitMetricResponse initMetricResponse) {
        this.db = initMetricResponse.db();
        this.namespace = initMetricResponse.namespace();
        this.metric = initMetricResponse.metric();
        this.completedSuccessfully = initMetricResponse.completedSuccessfully();
        this.errorMsg = initMetricResponse.errorMsg();
    }

    /**
     * @return the response db.
     */
    public String getDb() {
        return db;
    }

    /**
     * @return the response namespace.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return the response metric.
     */
    public String getMetric() {
        return metric;
    }

    /**
     * @return true if the request is completed successfully, false otherwise.
     */
    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    /**
     * @return comma separated errorMsg list, if the request is not successfully completed.
     */
    public String getErrorMsg() {
        return errorMsg;
    }
}
