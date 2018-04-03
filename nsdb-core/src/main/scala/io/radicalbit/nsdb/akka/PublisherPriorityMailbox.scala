package io.radicalbit.nsdb.akka

import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}

/**
  * Priority Mailbox with the purpose to give a higher priority to [[SubscribeBySqlStatement]], [[SubscribeByQueryId]] and [[Unsubscribe]] compared to [[io.radicalbit.nsdb.protocol.MessageProtocol.Commands.PublishRecord]].
  * In case [[io.radicalbit.nsdb.actors.PublisherActor]] is under pressure by a load of [[io.radicalbit.nsdb.protocol.MessageProtocol.Commands.PublishRecord]],
  * this mailbox guarantees that it is able to serve every subscription requests, that otherwise, would most likely fail for a time out.
  */
class PublisherPriorityMailbox(settings: akka.actor.ActorSystem.Settings, config: com.typesafe.config.Config)
    extends UnboundedPriorityMailbox(PriorityGenerator {
      case SubscribeBySqlStatement(_, _, _) => 0

      case SubscribeByQueryId(_, _) => 0

      case Unsubscribe => 1

      case _ ⇒ 2
    })
