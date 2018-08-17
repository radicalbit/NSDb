package io.radicalbit.nsdb.minicluster
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait MiniClusterSpec extends FunSuite with BeforeAndAfterAll {

  val nodesNumber: Int

  lazy val minicluster: NsdbMiniCluster = new MiniClusterStarter(nodesNumber)

  override def beforeAll(): Unit = {
    minicluster.start()
  }

  override def afterAll(): Unit = {
    minicluster.stop()
  }

  protected lazy val indexingTime: Long =
    minicluster.nodes.head.system.settings.config.getDuration("nsdb.write.scheduler.interval").toMillis

  protected def waitIndexing(): Unit = Thread.sleep(indexingTime + 1000)
}