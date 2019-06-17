package io.radicalbit.nsdb.minicluster
import java.time.Duration

import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait MiniClusterSpec extends FunSuite with BeforeAndAfterAll {

  val nodesNumber: Int

  implicit val formats = DefaultFormats

  def passivateAfter: Duration = Duration.ofHours(1)

  lazy val minicluster: NsdbMiniCluster = new MiniClusterStarter(nodesNumber)

  override def beforeAll(): Unit = {
    minicluster.start(true)
  }

  override def afterAll(): Unit = {
    minicluster.stop()
  }

  protected lazy val indexingTime: Long =
    minicluster.nodes.head.system.settings.config.getDuration("nsdb.write.scheduler.interval").toMillis

  protected def waitIndexing(): Unit    = Thread.sleep(indexingTime + 1000)
  protected def waitPassivation(): Unit = Thread.sleep(passivateAfter.toMillis + 1000)
}
