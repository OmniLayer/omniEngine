#!/bin/sh
set -x -e
HOST=omniwallet-test.ccbz9vycl90c.us-west-2.rds.amazonaws.com
CREATEOPTIONS="--host=$HOST --port=5432 --username=xmcmaster"
OPTIONS="$CREATEOPTIONS --echo-all -v ON_ERROR_STOP=1"
echo "Create Database..."
createdb $CREATEOPTIONS "omniwallet" "Omniwallet wallet and transaction database"
echo "Init Schema..."
psql $OPTIONS --dbname="omniwallet" -f ../omni_db_schema.psql
echo "Add intial data..."
psql $OPTIONS --dbname="omniwallet" -f ../omni_db_initialize_data.psql


