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

package io.radicalbit.nsdb.security;

import java.util.List;
import java.util.Map;

public interface JWTAuthorizationProvider extends NSDbAuthorizationProvider {

    @Override
    default String extractHttpSecurityPayload(Map<String, String> rawHeaders) {
        return rawHeaders.get("Authorization");
    }

    @Override
    default String extractWsSecurityPayload(List<String> subProtocols) {
        return subProtocols.stream().reduce("",  (a, b) -> a + "" + b);
    }

    @Override
    default String getGrpcSecurityHeader() {
        return "Authorization";
    }
}