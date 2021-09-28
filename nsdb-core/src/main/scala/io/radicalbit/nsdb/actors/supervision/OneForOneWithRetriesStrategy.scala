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

package io.radicalbit.nsdb.actors.supervision

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume, Stop}
import akka.actor.{ActorContext, ActorRef, ChildRestartStats, SupervisorStrategy}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.Duration

/**
  * Applies the fault handling `Directive` (Resume, Restart, Stop) specified in the `Decider`
  * to the child actor that failed, as opposed to [[akka.actor.AllForOneStrategy]] that applies
  * it to all children.
  *
  * @param maxNrOfRetries the number of times a child actor is allowed to be restarted or resumed, negative value means no limit
  *  if the duration is infinite. If the limit is exceeded the child actor is stopped
  * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window
  * @param loggingEnabled the strategy logs the failure if this is enabled (true), by default it is enabled
  * @param decider mapping from Throwable to [[akka.actor.SupervisorStrategy.Directive]], you can also use a
  *   [[scala.collection.immutable.Seq]] of Throwables which maps the given Throwables to restarts, otherwise escalates.
  * @param shutdownBehaviour behavoiur to be executed when the number or restart or retry attempts is reached.
  */
case class OneForOneWithRetriesStrategy(maxNrOfRetries: Int = -1,
                                        withinTimeRange: Duration = Duration.Inf,
                                        override val loggingEnabled: Boolean = true)(
    val decider: SupervisorStrategy.Decider)(val shutdownBehaviour: (ActorContext, ActorRef) => Unit)
    extends SupervisorStrategy {

  private val childrenResumes = new ConcurrentHashMap[String, Int]()

  def withinTimeRangeOption(withinTimeRange: Duration): Option[Duration] =
    if (withinTimeRange.isFinite && withinTimeRange >= Duration.Zero) Some(withinTimeRange) else None

  def maxNrOfRetriesOption(maxNrOfRetries: Int): Option[Int] =
    if (maxNrOfRetries < 0) None else Some(maxNrOfRetries)

  private val retriesWindow =
    (maxNrOfRetriesOption(maxNrOfRetries), withinTimeRangeOption(withinTimeRange).map(_.toMillis.toInt))

  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()

  override def processFailure(context: ActorContext,
                              restart: Boolean,
                              child: ActorRef,
                              cause: Throwable,
                              stats: ChildRestartStats,
                              children: Iterable[ChildRestartStats]): Unit =
    if (restart && stats.requestRestartPermission(retriesWindow))
      restartChild(child, cause, suspendFirst = false)
    else
      shutdownBehaviour(context, child)

  override def handleFailure(context: ActorContext,
                             child: ActorRef,
                             cause: Throwable,
                             stats: ChildRestartStats,
                             children: Iterable[ChildRestartStats]): Boolean = {
    val directive: SupervisorStrategy.Directive = decider.applyOrElse(cause, (_: Any) => Escalate)
    directive match {
      case Resume =>
        logFailure(context, child, cause, directive)
        if (maxNrOfRetries > -1)
          if (Option(childrenResumes.get(child.path.toSerializationFormat)).forall { _ < maxNrOfRetries }) {
            childrenResumes.put(child.path.toSerializationFormat,
                                Option(childrenResumes.get(child.path.toSerializationFormat)).map(_ + 1).getOrElse(1))
            resumeChild(child, cause)
          } else
            shutdownBehaviour(context, child)
        else
          resumeChild(child, cause)
        true
      case Restart =>
        logFailure(context, child, cause, directive)
        processFailure(context, true, child, cause, stats, children)
        true
      case Stop =>
        logFailure(context, child, cause, directive)
        processFailure(context, false, child, cause, stats, children)
        true
      case Escalate =>
        logFailure(context, child, cause, directive)
        false
    }
  }

}
