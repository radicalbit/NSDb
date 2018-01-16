package io.radicalbit.nsdb.web.client

import scala.io.StdIn

object WSClientApp extends App {

  var client: Option[WSClient] = if (args.length == 2) {
    Some(WSClient(args(0), args(1)))
  } else if (args.length == 3) {
    Some(WSClient(args(0), args(1), args(2), args(3)))
  } else {
    println("must provide 2 or 3 arguments")
    None
  }

  client.foreach { c =>
    c.connect()
    StdIn.readLine()
  }
}
