import io.radicalbit.nsdb.minicluster.{MiniClusterStarter, NsdbMiniCluster}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
trait MiniclusterSpec extends FunSuite with BeforeAndAfterAll {

  lazy val minicluster: NsdbMiniCluster = new MiniClusterStarter(2)

  override def beforeAll(): Unit = {
    minicluster.start()
  }

  override def afterAll(): Unit = {
    minicluster.stop()
  }
}
