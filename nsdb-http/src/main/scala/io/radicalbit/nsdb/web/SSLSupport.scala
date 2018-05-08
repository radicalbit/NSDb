/*
 * Copyright 2018 Radicalbit S.r.l.
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

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.Paths
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import com.typesafe.config.ConfigFactory

/**
  * Trait handling SSL/TLS configuration.
  */
trait SSLSupport {

  val sslConfig = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "https.conf").toFile)
    .resolve()
    .withFallback(ConfigFactory.load("https.conf"))

  /**
    * Read SSL/TLS boolean configuration that enable/disable this protocol.
    * @return boolean flag
    */
  def isSSLEnabled = sslConfig.getBoolean("ssl.enabled")

  /**
    * Builds [[HttpsConnectionContext]] in case SSL/TLS protocol is enable.
    * Standard configuration uses self-signed certificates.
    * More comprehensive documentation and a certificate creation guide can be found in official documentation.
    * @return [[HttpsConnectionContext]] with defined configuration
    */
  def serverContext: HttpsConnectionContext = {
    val password = sslConfig.getString("ssl.http.keyManager.store.password").toCharArray

    val ks: KeyStore = KeyStore.getInstance(sslConfig.getString("ssl.http.keyManager.store.type"))
    val keystore: InputStream =
      new FileInputStream(new File(sslConfig.getString("ssl.http.keyManager.store.name")))

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
