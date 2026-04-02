#!/bin/bash
# Generate self-signed SSL certificate for local development
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=IN/ST=TamilNadu/L=Chennai/O=SocketProject/CN=127.0.0.1"
echo "[CERTS] cert.pem and key.pem generated successfully."
