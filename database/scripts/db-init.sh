#!/bin/sh
set -x -e
OPTIONS="--echo-all -v ON_ERROR_STOP=1"
echo "Init Schema..."
psql $OPTIONS -f ../omni_db_schema.psql postgres
echo "Add intial data..."
psql $OPTIONS -f ../omni_db_initialize_data.psql omniwallet


