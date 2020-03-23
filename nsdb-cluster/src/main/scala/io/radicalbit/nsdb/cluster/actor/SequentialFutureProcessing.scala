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
import io.radicalbit.nsdb.cluster.actor.SequentialFutureProcessing.{Continue, PipeableFutureWithContinue}
import io.radicalbit.nsdb.common.protocol.NSDbSerializable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Enable sequential future processing in actors by leveraging behaviours.
  * {{{
  *   case Message =>
  *   sequential {
  *     Future { // computation}
  *   }.pipeTo(sender)
  * }}}
  *
  */
trait SequentialFutureProcessing extends Stash { this: Actor =>

  private val waitingBehaviour: Receive = {
    case Continue =>
      unstashAll()
      context.unbecome()
    case _ => stash
  }

  /**
    * Switch behaviour and return a future wrapper [[PipeableFutureWithContinue]] which is able to resume the old one.
    */
  def sequential[T](future: Future[T])(implicit executionContext: ExecutionContext): PipeableFutureWithContinue[T] = {
    context.become(waitingBehaviour)
    new PipeableFutureWithContinue(future, self)
  }

}

object SequentialFutureProcessing {

  case object Continue extends NSDbSerializable

  /**
    * Future wrapper that triggers the a sender actor by sending it a message.
    * @param future the original future.
    * @param sender the actor that process the future.
    */
  final private[SequentialFutureProcessing] class PipeableFutureWithContinue[T](val future: Future[T], sender: ActorRef)(
      implicit executionContext: ExecutionContext) {

    /**
      * Pipe the future to a recipient and trigger the sender.
      * @param recipient the recipient actor.
      * @param sender the actor that processed the future.
      */
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

    /**
      * Pipe the future to a recipient, trigger the sender, and execute an effect.
      * @param recipient the recipient actor.
      * @param effect the code that will be execute after the future completion.
      * @param sender the actor that processed the future.
      */
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
}
