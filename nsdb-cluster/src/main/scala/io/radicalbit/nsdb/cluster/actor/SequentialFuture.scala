package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorRef, Stash, Status}
import io.radicalbit.nsdb.cluster.actor.SequentialFuture.{PipeableFutureWithResume, Resume}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait SequentialFuture extends Stash { this: Actor =>

  def waitingBehaviour: Receive = {
    case _ => stash
    case Resume =>
      context.unbecome()
      unstashAll()
  }

  def sequential[T]( f : Future[T])(implicit executionContext: ExecutionContext) = {
    context.become(waitingBehaviour)
    new PipeableFutureWithResume(f, self)
  }

}

object SequentialFuture {

  private case object Resume

  final private class PipeableFutureWithResume[T](val future: Future[T],sender: ActorRef)(implicit executionContext: ExecutionContext) {

    def pipeTo(recipient: ActorRef)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
      future andThen {
        case Success(r) =>
          recipient ! r
          sender ! Resume
        case Failure(f) =>
          recipient ! Status.Failure(f)
          sender ! Resume
      }
    }

    def pipeToWithEffect(recipient: ActorRef)(effect: T => Unit)(
      implicit sender: ActorRef = Actor.noSender): Future[T] = {
      future andThen {
        case Success(r) =>
          effect(r)
          recipient ! r
          sender ! Resume
        case Failure(f) =>
          recipient ! Status.Failure(f)
          sender ! Resume
      }
    }
  }

}