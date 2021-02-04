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

package io.radicalbit.nsdb.client.rpc;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains factory methods to create most common instances of {@link TokenApplier}
 */
public class TokenAppliers {

    private static class JwtTokenApplier extends TokenApplier {

        public JwtTokenApplier(String token) {
            this.token = token;
        }

        @Override
        Map<String, String> formatToken(String token) {
            return new HashMap<String, String>() {{
                put("Authorization", MessageFormat.format("Bearer {0}", token));
            }};
        }

    }

    public static TokenApplier JWT(String token) {
        return new JwtTokenApplier(token);
    }

    public static TokenApplier Custom(String name, String value) {
        return new TokenApplier() {
            @Override
            Map<String, String> formatToken(String token) {
                return new HashMap<String, String>() {{
                    put(name, value);
                }};
            }
        };
    }
}
