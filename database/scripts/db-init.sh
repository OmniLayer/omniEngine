#!/bin/sh
set -x -e
OPTIONS="--echo-all -v ON_ERROR_STOP=1"
echo "Create Database..."
createdb "omniwallet" "Omniwallet wallet and transaction database"
echo "Init Schema..."
psql $OPTIONS -f ../omni_db_schema.psql omniwallet
echo "Add intial data..."
psql $OPTIONS -f ../omni_db_initialize_data.psql omniwallet


