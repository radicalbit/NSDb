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

package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorRef, Stash, Status}
import io.radicalbit.nsdb.cluster.actor.SequentialFuture.Continue
import io.radicalbit.nsdb.common.protocol.NSDbSerializable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait SequentialFuture extends Stash { this: Actor =>

  def waitingBehaviour: Receive = {
    case Continue =>
      unstashAll()
      context.unbecome()
    case _ => stash
  }

  def sequential[T](f: Future[T])(implicit executionContext: ExecutionContext) = {
    context.become(waitingBehaviour)
    new PipeableFutureWithResume(f, self)
  }

}

final class PipeableFutureWithResume[T](val future: Future[T], sender: ActorRef)(
    implicit executionContext: ExecutionContext) {

  def pipeTo(recipient: ActorRef)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
    future andThen {
      case Success(r) =>
        recipient ! r
        sender ! Continue
      case Failure(f) =>
        recipient ! Status.Failure(f)
        sender ! Continue
    }
    future
  }

  def pipeToWithEffect(recipient: ActorRef)(effect: T => Unit)(
      implicit sender: ActorRef = Actor.noSender): Future[T] = {
    future andThen {
      case Success(r) =>
        effect(r)
        recipient ! r
        sender ! Continue
      case Failure(f) =>
        recipient ! Status.Failure(f)
        sender ! Continue
    }
    future
  }

}

object SequentialFuture {

  case object Continue extends NSDbSerializable

}
