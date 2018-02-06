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

  def pipeToWithEffect(recipient: ActorRef)(effect: () => Unit)(
      implicit sender: ActorRef = Actor.noSender): Future[T] = {
    future andThen {
      case Success(r) ⇒
        effect()
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
