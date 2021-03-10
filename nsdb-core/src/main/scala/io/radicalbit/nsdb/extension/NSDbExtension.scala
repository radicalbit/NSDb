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

package io.radicalbit.nsdb.extension

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import com.typesafe.config.ConfigException
import io.radicalbit.nsdb.common.protocol.Bit

import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.Future

class NSDbExtension(system: ExtendedActorSystem) extends Extension {

  val log = Logging.getLogger(system, this)

  private val extensionConfig: Seq[String] = try {
    system.settings.config
      .getString("nsdb.extensions")
      .split(",")
  } catch {
    case _: ConfigException.Missing => Seq.empty
  }

  log.debug(s"extensions from configuration $extensionConfig")

  private val extensions: Seq[NSDbHook] =
    extensionConfig.map { className =>
      log.info(s"starting extension $className")
      Class.forName(className, true, ClassLoader.getSystemClassLoader).asSubclass(classOf[NSDbHook]).newInstance
    }

  import system.dispatcher

  //TODO add a name and decription property for the extensions (in the meta-inf or in some other way)
  def extensionsNames: Seq[String] = extensionConfig

  def insertBitHook(system: ActorSystem,
                    db: String,
                    namespace: String,
                    metric: String,
                    bit: Bit): Future[HookResult] = {
    Future
      .sequence(extensions.map { extension =>
        toScala(extension.insertBitHook(system, db, namespace, metric, bit)).recover {
          case t =>
            system.log.error(t, s"error during execution of extension $extension")
            HookResult.Failure(t.getMessage)
        }
      })
      .map(results =>
        results.foldLeft(HookResult.Success()) { (r1, r2) =>
          if (r1.isSuccess && r2.isSuccess) HookResult.Success()
          else
            HookResult.Failure(s"${r1.getFailureReason},${r2.getFailureReason}")
      })
  }

}

object NSDbExtension extends ExtensionId[NSDbExtension] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = NSDbExtension

  override def createExtension(system: ExtendedActorSystem): NSDbExtension =
    new NSDbExtension(system)
}
