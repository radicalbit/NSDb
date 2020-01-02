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

package io.radicalbit.nsdb.web.swagger

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.radicalbit.nsdb.web.routes.{CommandApi, DataApi, QueryApi}

class SwaggerDocService(address: String, port: Int) extends SwaggerHttpService {

  override val host                      = s"$address:$port" //the url of your api, not swagger's json endpoint
  override val basePath                  = "/" //the basePath for the API you are exposing
  override val apiDocsPath               = "api-docs" //where you want the swagger-json endpoint exposed
  override val info                      = Info() //provides license and other description details
  override val apiClasses: Set[Class[_]] = Set(classOf[CommandApi], classOf[QueryApi], classOf[DataApi])
}
