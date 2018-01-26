# SSL/TLS Configuration for HTTP endpoints

SSL/TLS over http is enable in ssl-http.conf configuration file.
In this file are present the following configuration:
- ` enable ` : boolean flag tho enable/disable https conf.
- ` protocol ` : TLS , it would have been hardcoded being standard choice.
- `keyManager.store.name` : external file containing server cert. It must be located in `/opt/certs` by default with extension specified at key `keyManager.store.type` .
- `keyManager.store.password` : password used to encrypt keystore file containing certificates. Use this password in certificates creation process.
- other settings are used by java API but shouldn't be changed without deep knowledge.

In the current settings self signed certificates are used so clients must trust selfsigned certs until an external CA will be used.
Using https , https server starts on port specified in cluster.conf at key `nsdb.http.https-port`.

### Generate self-signed certificate
```
openssl genrsa -des3 -out server.key 2048
openssl rsa -in server.key -out server.key
openssl req -sha256 -new -key server.key -out server.csr -subj '/CN=[your DomainName]'
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
cat server.crt server.key > cert.pem
openssl pkcs12 -export -in cert.pem -out nsdb.keystore.p12 -name nsdb-test
```
Note: as password use the one specified in `keyManager.store.password`

# SSL/TLS Configuration for intra-node communication

Akka intra-node SSL/TLS communication can be enabled configuration setting to true
`akka.remote.netty.tcp.enable-ssl` config.
In order to allow SSL/TLS protocols keystore and truststore must be defined in akka config file.
Default values locate :
- key-store in `/opt/certs/server.keystore`
- trust-store in `/opt/certs/server.truststore`
- both with password `nsdb.key`

### Generate sef-signed keystore and truststore certificates

Move in the configuration certs directory and execute the following commands:

```
openssl genrsa -out diagserverCA.key 2048
openssl req -x509 -new -nodes -key diagserverCA.key -sha256 -days 1024 -out diagserverCA.pem
openssl pkcs12 -export -name server-cert \
               -in diagserverCA.pem -inkey diagserverCA.key \
               -out serverkeystore.p12
keytool -importkeystore -destkeystore server.keystore \
                -srckeystore serverkeystore.p12 -srcstoretype pkcs12 \
                -alias server-cert
keytool -import -alias server-cert \
                -file diagserverCA.pem -keystore server.truststore
```
Key-store and trust-store password are defined in configuration at keys:
`key-store-password` and `trust-store-password` .

