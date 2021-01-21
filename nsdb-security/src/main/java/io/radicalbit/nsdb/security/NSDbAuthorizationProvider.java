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

public interface NSDbAuthorizationProvider {

    class AuthorizationResponse {

        private final boolean success;
        private final String failReason;

        public AuthorizationResponse(boolean success, String failReason) {
            this.success = success;
            this.failReason = failReason;
        }

        public AuthorizationResponse(boolean success) {
            this.success = success;
            this.failReason = "";
        }

        public boolean isSuccess() {
            return success;
        }

        public String getFailReason() {
            return failReason;
        }
    }

    static NSDbAuthorizationProvider empty() {
        return new EmptyNSDbAuthorizationProvider();
    }

    String extractHttpSecurityPayload(Map<String, String> rawHeaders);

    String extractWsSecurityPayload(List<String> subProtocols);

    AuthorizationResponse checkDbAuth(String db, String payload, boolean writePermission);

    AuthorizationResponse checkNamespaceAuth(String db, String namespace, String payload, boolean writePermission);

    AuthorizationResponse checkMetricAuth(String db, String namespace, String metric, String payload, boolean writePermission);

}
