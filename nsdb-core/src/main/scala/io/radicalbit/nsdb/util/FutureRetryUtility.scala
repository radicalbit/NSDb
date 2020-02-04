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

import akka.actor.{Actor, ActorRef, Scheduler, Status}
import akka.event.LoggingAdapter
import akka.pattern.after

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Utility Component for handling future retry mechanism
  */
trait FutureRetryUtility {

  implicit class FutureRetry[T](f: => Future[T]) {
    def retry(delay: FiniteDuration, retries: Int)(
        wasSuccessful: T => Boolean)(implicit ec: ExecutionContext, s: Scheduler, log: LoggingAdapter): Future[T] =
      (for {
        a <- f
        result <- if (wasSuccessful(a) || retries < 1) Future(a)
        else { log.warning("{}. Retrying...", a); after(delay, s)(retry(delay, retries - 1)(wasSuccessful)) }
      } yield result) recoverWith {
        case t if retries > 0 =>
          log.warning("{}. Retrying...", t); after(delay, s)(retry(delay, retries - 1)(wasSuccessful))
      }
  }

  implicit class PipeToFutureRetry[T](f: => Future[T]) {
    def pipeTo(delay: FiniteDuration, retries: Int, recipient: ActorRef)(wasSuccessful: T => Boolean = _ => true)(
        implicit ec: ExecutionContext,
        s: Scheduler,
        log: LoggingAdapter,
        sender: ActorRef = Actor.noSender) =
      f.retry(delay, retries)(wasSuccessful) andThen {
        case Success(r) ⇒ recipient ! r
        case Failure(f) ⇒ recipient ! Status.Failure(f)
      }
  }
}
