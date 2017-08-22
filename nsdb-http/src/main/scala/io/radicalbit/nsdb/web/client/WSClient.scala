package io.radicalbit.nsdb.web.client

import java.net.URI

import io.radicalbit.nsdb.web.actor.StreamActor.RegisterQuery
import org.java_websocket.client.WebSocketClient
import org.java_websocket.drafts.Draft_17
import org.java_websocket.handshake.ServerHandshake
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

class WSClient(url: String, namespace: String, queryString: String)
    extends WebSocketClient(new URI(url), new Draft_17()) {

  override def onMessage(message: String): Unit = println(message)

  override def onError(ex: Exception): Unit = println("Websocket Error: " + ex.getMessage)

  override def onClose(code: Int, reason: String, remote: Boolean): Unit = println("Websocket closed")

  override def onOpen(handshakedata: ServerHandshake): Unit = {

    implicit val formats = DefaultFormats

    println("Websocket opened")
    val registerQueryMessage = RegisterQuery(namespace, queryString)
    send(write(registerQueryMessage))
  }

}

object WSClient {
  def apply(url: String, namespace: String, queryString: String): WSClient = {
    new WSClient(url, namespace, queryString)
  }
}
