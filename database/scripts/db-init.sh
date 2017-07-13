#!/bin/bash
set -x -e
if [[ -z "$OMNIDB_ENGINE_PASSWORD" ]] || [[ -z "$OMNIDB_WWW_PASSWORD" ]] || [ -z "$OMNIDB_DATABASE" ] ; then
 echo "Environment variables OMNIDB_ENGINE_PASSWORD, OMNIDB_WWW_PASSWORD, and OMNIDB_DATABASE must be set"
 exit 1
fi
OPTIONS="--echo-all -v ON_ERROR_STOP=1"
echo "Create Database..."
createdb "${OMNIDB_DATABASE}" "Omniwallet wallet and transaction database"
echo "Init Schema..."
psql $OPTIONS -f ../omni_db_schema.psql ${OMNIDB_DATABASE}
echo "Add intial data..."
psql $OPTIONS -f ../omni_db_initialize_data.psql ${OMNIDB_DATABASE}
echo "Configure users and permissions..."
psql $OPTIONS -f ../omni_db_createusers.psql \
    --variable=omniengine=${OMNIDB_ENGINE_USER} \
    --variable=omnienginePassword=\'${OMNIDB_ENGINE_PASSWORD}\' \
    --variable=omniwww=${OMNIDB_WWW_USER} \
    --variable=omniwwwPassword=\'${OMNIDB_WWW_PASSWORD}\' ${OMNIDB_DATABASE}

