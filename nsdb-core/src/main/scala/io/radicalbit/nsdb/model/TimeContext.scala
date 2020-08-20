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

package io.radicalbit.nsdb.model

import java.util.TimeZone

/**
  * Context in which a read or a write request is executed.
  * @param currentTime The reference timestamp to be used as wall clock time.
  * @param timezone the timezone to be used.
  */
case class TimeContext(currentTime: Long = System.currentTimeMillis(), timezone: TimeZone = TimeZone.getDefault)
