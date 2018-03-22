package io.radicalbit.nsdb.web.swagger

import java.nio.file.Paths

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import com.typesafe.config.{Config, ConfigFactory}
import io.radicalbit.nsdb.web.routes.{CommandApi, DataApi, QueryApi}

class SwaggerDocService extends SwaggerHttpService {

  val config = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "cluster.conf").toFile)
    .resolve()
    .withFallback(ConfigFactory.load("cluster.conf"))

  override val host                      = s"${config.getString("nsdb.http.interface")}:${config.getInt("nsdb.http.port")}" //the url of your api, not swagger's json endpoint
  override val basePath                  = "/" //the basePath for the API you are exposing
  override val apiDocsPath               = "api-docs" //where you want the swagger-json endpoint exposed
  override val info                      = Info() //provides license and other description details
  override val apiClasses: Set[Class[_]] = Set(classOf[CommandApi], classOf[QueryApi], classOf[DataApi])
}