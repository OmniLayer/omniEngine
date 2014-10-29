#!/bin/bash
set -x -e
if [[ -z "$OMNIDB_ENGINE_PASSWORD" ]] || [[ -z "$OMNIDB_WWW_PASSWORD" ]] ; then
 echo "Environment variables OMNIDB_ENGINE_PASSWORD and OMNIDB_WWW_PASSWORD must be set"
 exit 1
fi
OPTIONS="--echo-all -v ON_ERROR_STOP=1"
echo "Create Database..."
createdb "omniwallet" "Omniwallet wallet and transaction database"
echo "Init Schema..."
psql $OPTIONS -f ../omni_db_schema.psql omniwallet
echo "Add intial data..."
psql $OPTIONS -f ../omni_db_initialize_data.psql omniwallet
echo "Configure users and permissions..."
psql $OPTIONS -f ../omni_db_createusers.psql \
    --variable=omnienginePassword=\'${OMNIDB_ENGINE_PASSWORD}\' \
    --variable=omniwwwPassword=\'${OMNIDB_WWW_PASSWORD}\' omniwallet

