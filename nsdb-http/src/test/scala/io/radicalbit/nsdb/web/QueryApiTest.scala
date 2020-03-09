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

package io.radicalbit.nsdb.web

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.actor.FakeReadCoordinator
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.web.Formats._
import io.radicalbit.nsdb.web.QueryApiTest.FakeWriteCoordinator
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

object QueryApiTest {
  class FakeWriteCoordinator extends Actor {
    override def receive: Receive = {
      case msg: MapInput => sender() ! InputMapped(msg.db, msg.namespace, msg.metric, msg.record)
    }
  }
}

class QueryApiTest extends FlatSpec with Matchers with ScalatestRouteTest {

  val writeCoordinatorActor: ActorRef = system.actorOf(Props[FakeWriteCoordinator])
  val readCoordinatorActor: ActorRef  = system.actorOf(Props[FakeReadCoordinator])

  val secureAuthenticationProvider: NSDBAuthProvider  = new TestAuthProvider
  val emptyAuthenticationProvider: EmptyAuthorization = new EmptyAuthorization

  /*
        adds to formats a CustomSerializerForTest that serializes relative timestamp (now) with a fake
        fixed timestamp (0L) in order to make the unit test time-independent
   */
  implicit val formats
    : Formats = DefaultFormats ++ CustomSerializers.customSerializers + CustomSerializerForTest + BitSerializer

  val secureQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = secureAuthenticationProvider

    override def readCoordinator: ActorRef = readCoordinatorActor
    override implicit val timeout: Timeout = 5 seconds

  }

  val emptyQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def readCoordinator: ActorRef = readCoordinatorActor
    override implicit val timeout: Timeout = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyQueryApi.queryApi()(system.log, formats)
  )

  val testSecuredRoutes = Route.seal(
    secureQueryApi.queryApi()(system.log, formats)
  )

  "QueryApi" should "not allow get" in {
    Get("/query") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "QueryApi" should "correctly query the db with a single filter over Long" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db with a single filter and with time range" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                Some(100),
                Some(200),
                Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db with a single filter with compatible type" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", "1", FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
    }

  }

  "QueryApi" should "fail with a single filter with incompatible type" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", "vd", FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe InternalServerError
    }
  }

  "QueryApi" should "correctly query the db with time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", Some(100), Some(200), None, None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
        |  "records" : [ {
        |    "timestamp" : 100,
        |    "value" : 1,
        |    "dimensions" : {
        |      "name" : "name",
        |      "number" : 2
        |    },
        |    "tags" : {
        |      "country" : "country"
        |    }
        |  }, {
        |    "timestamp" : 200,
        |    "value" : 3,
        |    "dimensions" : {
        |      "name" : "name",
        |      "number" : 2
        |    },
        |    "tags" : {
        |      "country" : "country"
        |    }
        |  } ]
        |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db without time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", None, None, None, None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a very simple query and return the parsed query" in {
    val q =
      QueryBody("db", "namespace", "metric", "select count(*) from metric", None, None, None, Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a not null condition and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select count(*) from metric where name is not null limit 1",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    },
          |    "condition" : {
          |      "expression" : {
          |        "expression" : {
          |          "dimension" : "name",
          |          "comparison" : "null"
          |        },
          |        "operator" : "not"
          |      }
          |    },
          |    "limit" : {
          |      "value" : 1
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a null condition and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select count(*) from metric where name is null limit 1",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    },
          |    "condition" : {
          |      "expression" : {
          |        "dimension" : "name",
          |        "comparison" : "null"
          |      }
          |    },
          |    "limit" : {
          |      "value" : 1
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple count aggregation query and return the parsed query" in {
    val q =
      QueryBody("db", "namespace", "metric", "select count(*) from metric limit 1", None, None, None, Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    },
          |    "limit" : {
          |      "value" : 1
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple sum aggregation query and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select sum(value) from metric group by country limit 2 ",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "value",
          |        "aggregation" : "sum"
          |      } ]
          |    },
          |    "groupBy" : {
          |      "field" : "country"
          |    },
          |    "limit" : {
          |      "value" : 2
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple min aggregation query and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select min(value) from metric group by country limit 3 ",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "value",
          |        "aggregation" : "min"
          |      } ]
          |    },
          |    "groupBy" : {
          |      "field" : "country"
          |    },
          |    "limit" : {
          |      "value" : 3
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple max aggregation query and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select max(value) from metric group by country limit 4",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "value",
          |        "aggregation" : "max"
          |      } ]
          |    },
          |    "groupBy" : {
          |      "field" : "country"
          |    },
          |    "limit" : {
          |      "value" : 4
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with asc order and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric order by timestamp limit 2",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "order" : {
          |      "order_by" : "timestamp",
          |      "direction" : "asc"
          |    },
          |    "limit" : {
          |      "value" : 2
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with desc order and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric order by timestamp desc limit 5",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "order" : {
          |      "order_by" : "timestamp",
          |      "direction" : "desc"
          |    },
          |    "limit" : {
          |      "value" : 5
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with 'and' condition return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric where timestamp > 0 and timestamp < 200  limit 2",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "condition" : {
          |      "expression" : {
          |        "expression1" : {
          |          "dimension" : "timestamp",
          |          "comparison" : ">",
          |          "value" : {
          |            "value" : 0
          |          }
          |        },
          |        "operator" : "and",
          |        "expression2" : {
          |          "dimension" : "timestamp",
          |          "comparison" : "<",
          |          "value" : {
          |            "value" : 200
          |          }
          |        }
          |      }
          |    },
          |    "limit" : {
          |      "value" : 2
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with equality expression and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric where timestamp = 5 limit 2",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "condition" : {
          |      "expression" : {
          |        "dimension" : "timestamp",
          |        "comparison" : "=",
          |        "value" : 5
          |      }
          |    },
          |    "limit" : {
          |      "value" : 2
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with some conditions and return the parsed query" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select * from metric where timestamp > 1 and timestamp < 2 and timestamp = 3 or timestamp >= 4 or timestamp <= 5",
        None,
        None,
        None,
        Some(true)
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
      |  "records" : [ {
      |    "timestamp" : 0,
      |    "value" : 1,
      |    "dimensions" : {
      |      "name" : "name",
      |      "number" : 2
      |    },
      |    "tags" : {
      |      "country" : "country"
      |    }
      |  }, {
      |    "timestamp" : 2,
      |    "value" : 3,
      |    "dimensions" : {
      |      "name" : "name",
      |      "number" : 2
      |    },
      |    "tags" : {
      |      "country" : "country"
      |    }
      |  } ],
      |  "parsed" : {
      |    "db" : "db",
      |    "namespace" : "namespace",
      |    "metric" : "metric",
      |    "distinct" : false,
      |    "fields" : { },
      |    "condition" : {
      |      "expression" : {
      |        "expression1" : {
      |          "dimension" : "timestamp",
      |          "comparison" : ">",
      |          "value" : {
      |            "value" : 1
      |          }
      |        },
      |        "operator" : "and",
      |        "expression2" : {
      |          "expression1" : {
      |            "dimension" : "timestamp",
      |            "comparison" : "<",
      |            "value" : {
      |              "value" : 2
      |            }
      |          },
      |          "operator" : "and",
      |          "expression2" : {
      |            "expression1" : {
      |              "dimension" : "timestamp",
      |              "comparison" : "=",
      |              "value" : 3
      |            },
      |            "operator" : "or",
      |            "expression2" : {
      |              "expression1" : {
      |                "dimension" : "timestamp",
      |                "comparison" : ">=",
      |                "value" : {
      |                  "value" : 4
      |                }
      |              },
      |              "operator" : "or",
      |              "expression2" : {
      |                "dimension" : "timestamp",
      |                "comparison" : "<=",
      |                "value" : {
      |                  "value" : 5
      |                }
      |              }
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query with like condition and return the parsed query" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select * from metric where name like $m$ and timestamp < 2 and timestamp = 3 or timestamp >= 4 or timestamp <= 5",
        None,
        None,
        None,
        Some(true)
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "condition" : {
          |      "expression" : {
          |        "expression1" : {
          |          "dimension" : "name",
          |          "comparison" : "like",
          |          "value" : "$m$"
          |        },
          |        "operator" : "and",
          |        "expression2" : {
          |          "expression1" : {
          |            "dimension" : "timestamp",
          |            "comparison" : "<",
          |            "value" : {
          |              "value" : 2
          |            }
          |          },
          |          "operator" : "and",
          |          "expression2" : {
          |            "expression1" : {
          |              "dimension" : "timestamp",
          |              "comparison" : "=",
          |              "value" : 3
          |            },
          |            "operator" : "or",
          |            "expression2" : {
          |              "expression1" : {
          |                "dimension" : "timestamp",
          |                "comparison" : ">=",
          |                "value" : {
          |                  "value" : 4
          |                }
          |              },
          |              "operator" : "or",
          |              "expression2" : {
          |                "dimension" : "timestamp",
          |                "comparison" : "<=",
          |                "value" : {
          |                  "value" : 5
          |                }
          |              }
          |            }
          |          }
          |        }
          |      }
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a simple query relative value and return the parsed query" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric where timestamp > now - 2d",
                None,
                None,
                None,
                Some(true))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : { },
          |    "condition" : {
          |      "expression" : {
          |        "dimension" : "timestamp",
          |        "comparison" : ">",
          |        "value" : {
          |          "value" : 0,
          |          "operator" : "-",
          |          "quantity" : 2,
          |          "unitMeasure" : "d"
          |        }
          |      }
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a complex query return the parsed query" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select count(*) from metric where timestamp > now - 2d or name = 'a' and timestamp < 3 group by country",
        None,
        None,
        None,
        Some(true)
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    },
          |    "condition" : {
          |      "expression" : {
          |        "expression1" : {
          |          "dimension" : "timestamp",
          |          "comparison" : ">",
          |          "value" : {
          |            "value" : 0,
          |            "operator" : "-",
          |            "quantity" : 2,
          |            "unitMeasure" : "d"
          |          }
          |        },
          |        "operator" : "or",
          |        "expression2" : {
          |          "expression1" : {
          |            "dimension" : "name",
          |            "comparison" : "=",
          |            "value" : "a"
          |          },
          |          "operator" : "and",
          |          "expression2" : {
          |            "dimension" : "timestamp",
          |            "comparison" : "<",
          |            "value" : {
          |              "value" : 3
          |            }
          |          }
          |        }
          |      }
          |    },
          |    "groupBy" : {
          |      "field" : "country"
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a very complex query return the parsed query" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select count(*) from metric where name like $m$ or timestamp in (now - 2h, 7) and timestamp > now - 2d or name = 'a' and timestamp < 3 group by country",
        None,
        None,
        None,
        Some(true)
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "*",
          |        "aggregation" : "count"
          |      } ]
          |    },
          |    "condition" : {
          |      "expression" : {
          |        "expression1" : {
          |          "dimension" : "name",
          |          "comparison" : "like",
          |          "value" : "$m$"
          |        },
          |        "operator" : "or",
          |        "expression2" : {
          |          "expression1" : {
          |            "dimension" : "timestamp",
          |            "value1" : {
          |              "value" : 0,
          |              "operator" : "-",
          |              "quantity" : 2,
          |              "unitMeasure" : "h"
          |            },
          |            "value2" : {
          |              "value" : 7
          |            }
          |          },
          |          "operator" : "and",
          |          "expression2" : {
          |            "expression1" : {
          |              "dimension" : "timestamp",
          |              "comparison" : ">",
          |              "value" : {
          |                "value" : 0,
          |                "operator" : "-",
          |                "quantity" : 2,
          |                "unitMeasure" : "d"
          |              }
          |            },
          |            "operator" : "or",
          |            "expression2" : {
          |              "expression1" : {
          |                "dimension" : "name",
          |                "comparison" : "=",
          |                "value" : "a"
          |              },
          |              "operator" : "and",
          |              "expression2" : {
          |                "dimension" : "timestamp",
          |                "comparison" : "<",
          |                "value" : {
          |                  "value" : 3
          |                }
          |              }
          |            }
          |          }
          |        }
          |      }
          |    },
          |    "groupBy" : {
          |      "field" : "country"
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "QueryApi called with optional parameter parsed = true" should
    "correctly query the db with a very very complex query return the parsed query" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select name, timestamp, value from metric where name like $m$ or timestamp in (now - 2h, 7) and timestamp > now - 2d or " +
          "name = 'a' or timestamp <= now - 1s and value >= 5 and timestamp < 3 limit 5",
        None,
        None,
        None,
        Some(true)
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ],
          |  "parsed" : {
          |    "db" : "db",
          |    "namespace" : "namespace",
          |    "metric" : "metric",
          |    "distinct" : false,
          |    "fields" : {
          |      "fields" : [ {
          |        "name" : "name"
          |      }, {
          |        "name" : "timestamp"
          |      }, {
          |        "name" : "value"
          |      } ]
          |    },
          |    "condition" : {
          |      "expression" : {
          |        "expression1" : {
          |          "dimension" : "name",
          |          "comparison" : "like",
          |          "value" : "$m$"
          |        },
          |        "operator" : "or",
          |        "expression2" : {
          |          "expression1" : {
          |            "dimension" : "timestamp",
          |            "value1" : {
          |              "value" : 0,
          |              "operator" : "-",
          |              "quantity" : 2,
          |              "unitMeasure" : "h"
          |            },
          |            "value2" : {
          |              "value" : 7
          |            }
          |          },
          |          "operator" : "and",
          |          "expression2" : {
          |            "expression1" : {
          |              "dimension" : "timestamp",
          |              "comparison" : ">",
          |              "value" : {
          |                "value" : 0,
          |                "operator" : "-",
          |                "quantity" : 2,
          |                "unitMeasure" : "d"
          |              }
          |            },
          |            "operator" : "or",
          |            "expression2" : {
          |              "expression1" : {
          |                "dimension" : "name",
          |                "comparison" : "=",
          |                "value" : "a"
          |              },
          |              "operator" : "or",
          |              "expression2" : {
          |                "expression1" : {
          |                  "dimension" : "timestamp",
          |                  "comparison" : "<=",
          |                  "value" : {
          |                    "value" : 0,
          |                    "operator" : "-",
          |                    "quantity" : 1,
          |                    "unitMeasure" : "s"
          |                  }
          |                },
          |                "operator" : "and",
          |                "expression2" : {
          |                  "expression1" : {
          |                    "dimension" : "value",
          |                    "comparison" : ">=",
          |                    "value" : {
          |                      "value" : 5
          |                    }
          |                  },
          |                  "operator" : "and",
          |                  "expression2" : {
          |                    "dimension" : "timestamp",
          |                    "comparison" : "<",
          |                    "value" : {
          |                      "value" : 3
          |                    }
          |                  }
          |                }
          |              }
          |            }
          |          }
          |        }
          |      }
          |    },
          |    "limit" : {
          |      "value" : 5
          |    }
          |  }
          |}""".stripMargin

    }
  }

  "Secured QueryApi" should "not allow a request without the security header" in {
    val q = QueryBody("db", "namespace", "metric", "select from metric", Some(1), Some(2), None, None)

    Post("/query", q) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/query", q).withHeaders(RawHeader("wrong", "wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured QueryApi" should "not allow a request for an unauthorized resources" in {
    val q = QueryBody("db", "namespace", "notAuthorizedMetric", "select from metric", Some(1), Some(2), None, None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }
  }

  "Secured QueryApi" should "allow a request for an authorized resources" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", Some(1), Some(2), None, None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }
}
