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

import java.time.Duration;

/**
 * Models a MetricInfo result contained in the describe metric response
 *
 * see {@link io.radicalbit.nsdb.api.java.DescribeMetricResult} for more details.
 */
public class MetricInfoResult {

    private String metric;
    private Duration shardInterval;
    private Duration retention;

    public MetricInfoResult(String metric, Long shardInterval, Long retention) {
        this.metric = metric;
        this.shardInterval = Duration.ofMillis(shardInterval);
        this.retention = Duration.ofMillis(retention);
    }

    public String getMetric() {
        return metric;
    }

    public Duration getShardInterval() {
        return shardInterval;
    }

    public Duration getRetention() {
        return retention;
    }

    @Override
    public String toString() {
        return "MetricInfoResult{" +
                "metric='" + metric + '\'' +
                ", shardInterval=" + shardInterval +
                ", retention=" + retention +
                '}';
    }
}
