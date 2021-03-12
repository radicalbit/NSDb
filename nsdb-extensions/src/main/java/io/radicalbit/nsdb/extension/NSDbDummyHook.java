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

package io.radicalbit.nsdb.extension;

import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.radicalbit.nsdb.common.protocol.Bit;

import java.util.concurrent.CompletableFuture;

public class NSDbDummyHook implements NSDbHook {
    @Override
    public CompletableFuture<HookResult> insertBitHook(ActorSystem system, String securityPayload, String db, String namespace, String metric, Bit bit) {
        LoggingAdapter log = Logging.getLogger(system, this);
        log.info("Executing dummy extension");
        return CompletableFuture.completedFuture(HookResult.Success());
    }
}
