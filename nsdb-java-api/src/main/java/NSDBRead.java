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

import io.radicalbit.nsdb.api.java.NSDB;
import io.radicalbit.nsdb.api.java.QueryResult;

/**
 * This class is meant to be an example of a call to the execute Statement Apis.
 */
public class NSDBRead {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").query("select * from people limit 1");

        QueryResult result = nsdb.executeStatement(statement).get();

        if (result.isCompletedSuccessfully()) {
            System.out.println("db " + result.getDb());
            System.out.println("namespace " + result.getNamespace());
            System.out.println("metric " + result.getMetric());
            System.out.println("bits " + result.getRecords());
        } else {
            System.out.println("reason " + result.getReason());
        }
    }
}
