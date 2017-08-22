package io.radicalbit.nsdb.web.client

import scala.io.StdIn

object WSClientApp extends App {

  val client = WSClient("ws://localhost:9000/ws-stream", "registry", "select * from people limit 1")

  client.connect()

  StdIn.readLine()
}
