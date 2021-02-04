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

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Implements a Grpc {@link CallCredentials} in order to perform an authorized call.
 * An authorization header is set from a {@link TokenApplier#token} inside method {@link CallCredentials#applyRequestMetadata}
 */
public abstract class TokenApplier extends CallCredentials {

    private final Logger log = LoggerFactory.getLogger(TokenApplier.class);

    /**
     * The token to use to set the authorization header.
     */
    protected String token;

    abstract Map<String, String> formatToken(String token);


    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        appExecutor.execute(() -> {
            if (token == null || "".equals(token)) {
                applier.fail(Status.UNAUTHENTICATED.withDescription("Empty token provided"));
            } else {
                Map<String, String> rawHeaders = formatToken(token);
                if (rawHeaders.size() > 1) {
                    applier.fail(Status.UNAUTHENTICATED.withDescription("Token format function must return a 1-size map"));
                } else {
                    try {
                        Metadata metadata = new Metadata();
                        Map.Entry<String, String> rawHeader = rawHeaders.entrySet().iterator().next();
                        metadata.put(Metadata.Key.of(rawHeader.getKey(), Metadata.ASCII_STRING_MARSHALLER), rawHeader.getValue());
                        applier.apply(metadata);
                    } catch (Throwable t) {
                        String errorMsg = "An exception when obtaining JWT token";
                        log.error(errorMsg, t);
                        applier.fail(Status.UNAUTHENTICATED.withDescription(errorMsg).withCause(t));
                    }
                }
            }
        });
    }

    @Override
    public void thisUsesUnstableApi() {
    }
}
