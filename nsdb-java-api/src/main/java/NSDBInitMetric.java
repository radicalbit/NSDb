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

import io.radicalbit.nsdb.api.java.InitMetricResult;
import io.radicalbit.nsdb.api.java.NSDB;

/**
 * This class is meant to be an example of a call to the init metric Api.
 */
public class NSDBInitMetric {

    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.MetricInfo metricInfo = nsdb.db("root")
                .namespace("registry")
                .bit("people")
                .shardInterval("2d")
                .retention("1d");


        InitMetricResult result = nsdb.initMetric(metricInfo).get();
        System.out.println("IsSuccessful = " + result.isCompletedSuccessfully());
        System.out.println("errors = " + result.getErrorMsg());
    }
}
