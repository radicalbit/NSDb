package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.coordinator.ReadCoordinator.Commands.GetConnectedNodes
import io.radicalbit.nsdb.cluster.coordinator.ReadCoordinator.Events.ConnectedNodesGot
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future

/**
  * Actor that receives and handle every read request
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator
  * @param namespaceSchemaActor [[io.radicalbit.nsdb.cluster.actor.MetricsSchemaActor]] the metrics schema actor
  */
class ReadCoordinator(metadataCoordinator: ActorRef, namespaceSchemaActor: ActorRef)
    extends Actor
    with ActorLogging
    with NsdbPerfLogger {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinatoor.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  lazy val sharding: Boolean          = context.system.settings.config.getBoolean("nsdb.sharding.enabled")
  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  import context.dispatcher

  private val namespaces: mutable.Map[String, ActorRef] = mutable.Map.empty

  override def receive: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, nodeName) =>
      namespaces += (nodeName -> actor)
      sender() ! NamespaceDataActorSubscribed(actor, nodeName)
    case GetConnectedNodes =>
      sender ! ConnectedNodesGot(namespaces.keys.toSeq)
    case msg @ GetNamespaces(db) =>
      Future
        .sequence(namespaces.values.toSeq.map(actor => (actor ? msg).mapTo[NamespacesGot].map(_.namespaces)))
        .map(_.flatten.toSet)
        .map(namespaces => NamespacesGot(db, namespaces))
        .pipeTo(sender)
    case msg @ GetMetrics(db, namespace) =>
      Future
        .sequence(namespaces.values.toSeq.map(actor => (actor ? msg).mapTo[MetricsGot].map(_.metrics)))
        .map(_.flatten.toSet)
        .map(metrics => MetricsGot(db, namespace, metrics))
        .pipeTo(sender)
    case msg: GetSchema =>
      namespaceSchemaActor forward msg
    case ExecuteStatement(statement) =>
      val startTime = System.currentTimeMillis()
      log.debug("executing {} ", statement)
      (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
        .mapTo[SchemaGot]
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) =>
            Future
              .sequence(namespaces.values.toSeq.map(actor => actor ? ExecuteSelectStatement(statement, schema)))
              .map { seq =>
                val errs = seq.collect {
                  case e: SelectStatementFailed => e.reason
                }
                if (errs.isEmpty) {
                  val results = seq.asInstanceOf[Seq[SelectStatementExecuted]]
                  SelectStatementExecuted(statement.db,
                                          statement.namespace,
                                          statement.metric,
                                          results.flatMap(_.values))
                } else {
                  SelectStatementFailed(errs.mkString(","))
                }
              }
          case _ =>
            Future(
              SelectStatementFailed(s"Metric ${statement.metric} does not exist ", MetricNotFound(statement.metric)))
        }
        .recoverWith {
          case t => Future(SelectStatementFailed(t.getMessage))
        }
        .pipeToWithEffect(sender()) { _ =>
          if (perfLogger.isDebugEnabled)
            perfLogger.debug("executed statement {} in {} millis", statement, System.currentTimeMillis() - startTime)
        }
  }
}

object ReadCoordinator {

  object Commands {
    private[coordinator] case object GetConnectedNodes
  }

  object Events {
    private[coordinator] case class ConnectedNodesGot(nodes: Seq[String])
  }

  def props(metadataCoordinator: ActorRef, schemaActor: ActorRef): Props =
    Props(new ReadCoordinator(metadataCoordinator, schemaActor))

}
