#!/bin/bash

# cut off tcp:// from tcp://ip.ad.dr.ss:port
if [ -z "$COUCHDB_USER" ]; then
  echo "Warning: COUCHDB_USERNAME not set"
fi
if [ -z "$COUCHDB_PASSWORD" ]; then
  echo "Warning: COUCHDB_PASSWORD not set"
fi

while ! curl http://taskdb:5984/
do
  echo "Waiting for Couchdb..."
  sleep 1
done
while ! curl http://jobdb:5984/
do
  echo "Waiting for Couchdb..."
  sleep 1
done
echo "$(date) - connected successfully"

sudo -u xenon ". simcity/bin/activate && simcity init -u ${COUCHDB_USER} -p ${COUCHDB_PASSWORD}"

exec /sbin/my_init

