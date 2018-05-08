/*
 * Copyright 2018 Radicalbit S.r.l.
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

import akka.actor.{Actor, ActorRef, Status}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class PipeableFutureWithSideEffect[T](val future: Future[T])(implicit executionContext: ExecutionContext) {

  def pipeTo(recipient: ActorRef)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
    future andThen {
      case Success(r) ⇒ recipient ! r
      case Failure(f) ⇒ recipient ! Status.Failure(f)
    }
  }

  def pipeToWithEffect(recipient: ActorRef)(effect: T => Unit)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
    future andThen {
      case Success(r) ⇒
        effect(r)
        recipient ! r
      case Failure(f) ⇒ recipient ! Status.Failure(f)
    }
  }
}

object PipeableFutureWithSideEffect {
  implicit def pipe[T](future: Future[T])(
      implicit executionContext: ExecutionContext): PipeableFutureWithSideEffect[T] =
    new PipeableFutureWithSideEffect(future)
}
