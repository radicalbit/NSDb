package io.radicalbit.nsdb.test

import java.time.Duration

import io.radicalbit.nsdb.minicluster.NsdbMiniCluster
import org.json4s.DefaultFormats
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait MiniClusterSpec extends FunSuite with BeforeAndAfterAll with Eventually with NsdbMiniCluster {

  val nodesNumber: Int

  implicit val formats = DefaultFormats

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(20, Seconds))

  def passivateAfter: Duration = Duration.ofHours(1)

  override def beforeAll(): Unit = {
    start(true)
  }

  override def afterAll(): Unit = {
    stop()
  }

  protected lazy val indexingTime: Long =
    nodes.head.system.settings.config.getDuration("nsdb.write.scheduler.interval").toMillis

  protected def waitIndexing(): Unit    = Thread.sleep(indexingTime + 1000)
  protected def waitPassivation(): Unit = Thread.sleep(passivateAfter.toMillis + 1000)
}
