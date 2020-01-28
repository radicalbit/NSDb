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

package io.radicalbit.nsdb.util

import akka.actor.Scheduler
import akka.event.LoggingAdapter
import akka.pattern.after

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait RetryFutureUtility {
  implicit class RetryFuturePolicy[T](f: => Future[T]) {
    def retry(delay: FiniteDuration,
              retries: Int)(implicit ec: ExecutionContext, s: Scheduler, log: LoggingAdapter): Future[T] =
      f recoverWith {
        case t if retries > 0 => log.info(s"$t. Retrying..."); after(delay, s)(retry(delay, retries - 1))
      }
  }
}
