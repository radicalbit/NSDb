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

/**
 * Interface to extend in order to develop a custom authentication provider.
 */
public interface NSDbAuthorizationProvider {

    /**
     * Class that contains the results of an authorization process.
     */
    class AuthorizationResponse {


        private final boolean success;
        private final String failReason;

        /**
         * @param success: if authorization succeeded or not.
         * @param failReason: the fail reason.
         */
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

    default boolean isEmpty() {
        return this instanceof EmptyNSDbAuthorizationProvider;
    }

    /**
     * Extract the security info from Http headers.
     * @param rawHeaders Http headers in an agnostic format.
     * @return the security payload in string format.
     */
    String extractHttpSecurityPayload(Map<String, String> rawHeaders);

    /**
     * Extract the security info from Ws request subprotocols.
     * @param subProtocols list of all subprotocols.
     * @return the security payload in string format.
     */
    String extractWsSecurityPayload(List<String> subProtocols);

    /**
     * @return the header name that will be used to extract grpc security payload
     */
    String getGrpcSecurityHeader();

    /**
     * Checks if a request against a Db is authorized.
     * @param db the db to check.
     * @param payload the security payload gathered from request.
     * @param writePermission true if write permission is required.
     * @return the resulting {@link AuthorizationResponse}.
     */
    AuthorizationResponse checkDbAuth(String db, String payload, boolean writePermission);

    /**
     * Checks if a request against a Db is authorized.
     * @param db the db to check.
     * @param namespace the namespace to check.
     * @param payload the security payload gathered from request.
     * @param writePermission true if write permission is required.
     * @return the resulting {@link AuthorizationResponse}.
     */
    AuthorizationResponse checkNamespaceAuth(String db, String namespace, String payload, boolean writePermission);

    /**
     * Checks if a request against a Db is authorized.
     * @param db the db to check.
     * @param namespace the namespace to check.
     * @param metric the metric to check.
     * @param payload the security payload gathered from request.
     * @param writePermission true if write permission is required.
     * @return the resulting {@link AuthorizationResponse}.
     */
    AuthorizationResponse checkMetricAuth(String db, String namespace, String metric, String payload, boolean writePermission);

}
