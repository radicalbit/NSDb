package io.radicalbit.nsdb.web.swagger

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.radicalbit.nsdb.web.routes.{CommandApi, DataApi, QueryApi}

class SwaggerDocService extends SwaggerHttpService {
  override val host                      = "localhost:9000" //the url of your api, not swagger's json endpoint
  override val basePath                  = "/" //the basePath for the API you are exposing
  override val apiDocsPath               = "api-docs" //where you want the swagger-json endpoint exposed
  override val info                      = Info() //provides license and other description details
  override val apiClasses: Set[Class[_]] = Set(classOf[CommandApi], classOf[QueryApi], classOf[DataApi])
//  override val unwantedDefinitions = Seq("NSDBAuthProvider", "NSDBAuthProvider")

}
