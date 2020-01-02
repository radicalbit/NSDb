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

import io.radicalbit.nsdb.rpc.responseCommand.DescribeMetricResponse;
import scala.compat.java8.OptionConverters;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Class that models the describe metric response.
 */
public class DescribeMetricResult {

    private String db;
    private String namespace;
    private String metric;
    private List<SchemaField> fields;
    private MetricInfoResult metricInfo;
    private boolean completedSuccessfully;
    private String errors;


    /**
     * friendly Constructor that builds the object from the Grpc raw result
     * @param describeMetricResponse the Grpc result
     */
    DescribeMetricResult(DescribeMetricResponse describeMetricResponse) {
        this.db = describeMetricResponse.db();
        this.namespace = describeMetricResponse.namespace();
        this.metric = describeMetricResponse.metric();

        this.fields = scala.collection.JavaConversions.seqAsJavaList(describeMetricResponse.fields()).stream().map(f ->  new SchemaField(f.name(), f.fieldClassType().name())).collect(Collectors.toList());
        this.metricInfo = OptionConverters.toJava(describeMetricResponse.metricInfo()).map(mi -> new MetricInfoResult(this.metric, mi.shardInterval(), mi.retention())).orElse(new MetricInfoResult(this.metric, 0L, 0L));

        this.completedSuccessfully = describeMetricResponse.completedSuccessfully();
        this.errors = describeMetricResponse.errors();
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

    public List<SchemaField> getFields() {
        return fields;
    }

    public MetricInfoResult getMetricInfo() {
        return metricInfo;
    }

    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }

    public String getErrors() {
        return errors;
    }

    @Override
    public String toString() {
        return "DescribeMetricResult{" +
                "db='" + db + '\'' +
                ", namespace='" + namespace + '\'' +
                ", metric='" + metric + '\'' +
                ", fields=" + fields +
                ", metricInfo=" + metricInfo +
                ", completedSuccessfully=" + completedSuccessfully +
                ", errors='" + errors + '\'' +
                '}';
    }
}
