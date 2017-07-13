#!/bin/sh
set -x -e
if [ -z "$OMNIDB_DATABASE" ] ; then
 echo "Environment variable OMNIDB_DATABASE must be set"
 exit 1
fi

psql ${OMNIDB_DATABASE}


