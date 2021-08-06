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

package io.radicalbit.nsdb.api.java.example;

import io.radicalbit.nsdb.api.java.NSDB;

/**
 * This class is meant to be an example of a call to the execute Statement Apis.
 */
public class NSDBStreaming {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").metric("people").query("select * from people limit 1");

        nsdb.subscribe(statement,System.out::println);
    }
}
