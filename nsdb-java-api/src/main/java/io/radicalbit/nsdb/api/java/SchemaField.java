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

/**
 * Models a Schema field contained in the describe metric response
 *
 * see {@link io.radicalbit.nsdb.api.java.DescribeMetricResult} for more details.
 */
public class SchemaField {

    private String name;
    private String fieldType;

    public SchemaField(String name, String fieldType) {
        this.name = name;
        this.fieldType = fieldType;
    }

    public String getName() {
        return name;
    }

    public String getFieldType() {
        return fieldType;
    }

    @Override
    public String toString() {
        return "SchemaField{" +
                "name='" + name + '\'' +
                ", fieldType='" + fieldType + '\'' +
                '}';
    }
}
