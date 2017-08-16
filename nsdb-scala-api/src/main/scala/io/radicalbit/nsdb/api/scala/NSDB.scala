package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.api.scala.NSDB.{host, port, schema}

import scala.concurrent.Future

object NSDB {

  private val schema = "akka"

  private val host = "loalhost"

  private val port = 5678

  def connect(host: String, port: Int) = new NSDB(host = host, port = port)

  implicit class MetricWriter(metric: Metric) {

    def write: Future[Boolean] = {
      // TODO: Use here the Client facilities
      Future.successful(true)
    }
  }
}

class NSDB(host: String, port: Int) {

  private def url(host: String = host, port: Int = port) = s"$schema://$host:$port"

  def metric(metric: String) = Metric(metric)
}

case class Metric(private val name: String,
                  private val fields: List[Field] = List.empty[Field],
                  private val dimensions: List[Dimension] = List.empty[Dimension]) {

  def field[T](name: String, value: T): Metric     = copy(name, fields :+ Field(name, value), dimensions)
  def dimension[T](name: String, value: T): Metric = copy(name, fields, dimensions :+ Dimension(name, value))
}

case class Field(name: String, value: Any)
case class Dimension(name: String, value: Any)
