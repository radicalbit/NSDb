# SSL/TLS Configuration for HTTP endpoints

SSL/TLS over http is enable in ssl-http.conf configuration file.
In this file are present the following configuration:
- ` enable ` : boolean flag tho enable/disable https conf.
- ` protocol ` : TLS , it would have been hardcoded being standard choice.
- `keyManager.store.name` : external file containing server cert. It must be located in `/opt/certs` with extension specified at key `keyManager.store.type` .
- `keyManager.store.password` : password used to encrypt keystore file containing certificates. Use this password in certificates creation process.
- other settings are used by java API but shouldn't be changed without deep knowledge.

In the current settings self signed certificates are used so clients must trust selfsigned certs until an external CA will be used.
Using https , https server starts on port specified in cluster.conf at key `nsdb.http.ssl-port`.

### Generate self-signed certificate
```
openssl genrsa -des3 -out server.key 2048
openssl rsa -in server.key -out server.key
openssl req -sha256 -new -key server.key -out server.csr -subj '/CN=localhost'
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
cat server.crt server.key > cert.pem
openssl pkcs12 -export -in cert.pem -out nsdb.keystore.p12 -name nsdb-test -noiter –nomaciter
```
Note: as password use the one specified in `keyManager.store.password`