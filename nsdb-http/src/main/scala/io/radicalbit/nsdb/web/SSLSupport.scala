package io.radicalbit.nsdb.web

import java.io.InputStream
import java.nio.file.Paths
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import com.typesafe.config.ConfigFactory

trait SSLSupport {

  val sslConfig = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "ssl.conf").toFile)
    .resolve()

  def isSSLEnabled = sslConfig.getBoolean("ssl.enabled")

  def serverContext: HttpsConnectionContext = {
    val password = sslConfig.getString("ssl.http.keyManager.store.password").toCharArray

    val ks: KeyStore = KeyStore.getInstance(sslConfig.getString("ssl.http.keyManager.store.type"))
    val keystore: InputStream =
      getClass.getResource(sslConfig.getString("ssl.http.keyManager.store.name")).openStream()

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory = KeyManagerFactory.getInstance(sslConfig.getString("ssl.http.keyManager.algorithm"))
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory =
      TrustManagerFactory.getInstance(sslConfig.getString("ssl.http.trustManager.algorithm"))
    tmf.init(ks)
    val sslContext: SSLContext = SSLContext.getInstance(sslConfig.getString("ssl.protocol"))
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)

    ConnectionContext.https(sslContext)
  }

}
