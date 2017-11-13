package io.radicalbit.nsdb.web

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import io.radicalbit.nsdb.core.{Core, CoreActors}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import scala.concurrent.duration._
import org.scalatest._

class QueryApiTest extends FlatSpec with Matchers with ScalatestRouteTest with QueryResources {

  implicit val formats          = DefaultFormats
  implicit val timeout: Timeout = 5 seconds

  val testRoutes = Route.seal(
    queryResources(null, null)
  )

  //  "Cache service" should "get a mapping available in cache" in {
  //    Get("/api/v0.1/cache/com.customer.events.Purchase/1") ~> testRoutes ~> check {
  //      status shouldBe OK
  //      val entity = entityAs[JValue]
  //      val name = entity \ "name"
  //      val namespace = entity \ "namespace"
  //      name shouldBe an[JString]
  //      namespace shouldBe an[JString]
  //      name.extract[String] should equal("Purchase")
  //      namespace.extract[String] should equal("com.customer.events")
  //    }
  //  }

  "QueryApi" should "not allow get" in {
    Get("/query") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

//  it should "return 404 with a wrong version" in {
//    Get("/api/v0.1/cache/com.customer.events.Purchase/2") ~> testRoutes ~> check {
//      status shouldEqual NotFound
//    }
//  }

  "QueryApi" should "correctly query the db" in {
    val q = QueryBody("namespace", "select * from metric limit 1", None, None)

    Post("/query", q) ~> testRoutes ~> check {
      println(status)
      println(response)
      status shouldBe OK
    }

    //    Get("/api/v0.1/cache/new.namespace.Purchase/1") ~> testRoutes ~> check {
    //      status shouldBe OK
    //      val entity = entityAs[JValue]
    //      val name = entity \ "name"
    //      val namespace = entity \ "namespace"
    //      name shouldBe an[JString]
    //      namespace shouldBe an[JString]
    //      name.extract[String] should equal("Purchase")
    //      namespace.extract[String] should equal("new.namespace")
    //    }
    //  }

    "QueryApi" should "delete a mapping in cache" in {
      Delete("/api/v0.1/cache/com.customer.events.Purchase/1").~>(testRoutes)(TildeArrow.injectIntoRoute) ~> check {
        status shouldBe OK
      }

      //    Get("/api/v0.1/cache/com.customer.events.Purchase/1") ~> testRoutes ~> check {
      //      status shouldBe NotFound
      //    }
    }

    //  private def fromInputStream(path: String) =
    //    scala.io.Source.fromInputStream(this.getClass.getResourceAsStream(s"$path"))("UTF-8").mkString
  }
}
