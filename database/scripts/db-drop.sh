#!/bin/sh
set -x -e
if [[ -z "$OMNIDB_ENGINE_PASSWORD" ]] || [[ -z "$OMNIDB_WWW_PASSWORD" ]] || [ -z "$OMNIDB_DATABASE" ] ; then
 echo "Environment variables OMNIDB_ENGINE_PASSWORD, OMNIDB_WWW_PASSWORD, and OMNIDB_DATABASE must be set"
 exit 1
fi

dropdb --echo --if-exists  ${OMNIDB_DATABASE}
