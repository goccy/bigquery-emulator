# SSL Configuration
## `server.crt`
This is the SSL certificate used by the HTTPS server.

```bash
openssl req -new -x509 -sha256 -key server.key -out server.crt -days 3650
```

## `server.key`
Server SSL Key

```bash
openssl genrsa -out server.key 204
```


## `truststore.jks`
This Truststore can be used for connecting to the BigQuery emulator via JDBC.
It contains the BigQuery Emulator server certificate and the `oauth2.googleapis.com` certificate.

```bash
keytool -import -alias bigquery-emulator-localhost \
  -file server.crt -keystore truststore.jks -storepass "test@123"

keytool -import -alias oauth -file upload.video.google.com.cer \
  -keystore truststore.jks -storepass "test@123"
```


## `upload.video.google.com.crt`
Public certificate for `oauth2.googleapis.com`