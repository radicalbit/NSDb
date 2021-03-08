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

import java.util.Objects;

public class HookResult {

    private final boolean Success;
    private final String failureReason;

    private HookResult(boolean success, String failureReason) {
        Success = success;
        this.failureReason = failureReason;
    }

    public static HookResult Success() {
        return new HookResult(true, null);
    }

    public static HookResult Failure(String reason) {
        return new HookResult(false, reason);
    }

    public boolean isSuccess() {
        return Success;
    }

    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HookResult that = (HookResult) o;

        if (Success != that.Success) return false;
        return Objects.equals(failureReason, that.failureReason);
    }

    @Override
    public int hashCode() {
        int result = (Success ? 1 : 0);
        result = 31 * result + (failureReason != null ? failureReason.hashCode() : 0);
        return result;
    }
}
