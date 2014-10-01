#!/bin/sh
set -x -e
HOST=omniwallet-test.ccbz9vycl90c.us-west-2.rds.amazonaws.com
OPTIONS="--echo-all --host=$HOST --port=5432 --username=xmcmaster -v ON_ERROR_STOP=1"
echo "Init Schema..."
psql $OPTIONS --dbname="postgres" -f omni_db_schema.psql
echo "Add intial data..."
psql $OPTIONS --dbname="omniwallet" -f omni_db_initialize_data.psql


