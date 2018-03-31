package io.radicalbit.nsdb.akka

import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}

class PublisherPriorityMailbox(settings: akka.actor.ActorSystem.Settings, config: com.typesafe.config.Config)
    extends UnboundedPriorityMailbox(PriorityGenerator {
      case SubscribeBySqlStatement(_, _, _) => 0

      case SubscribeByQueryId(_, _) => 0

      case Unsubscribe => 1

      case _ ⇒ 2
    })
